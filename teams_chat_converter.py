import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import Tuple, List, Dict
import hashlib


class TeamsChatConverter:
    """
    Purview Teams HTML -> Excel parser.

    Supports:
    - Display names row for conversation participants
    - unknown-direction-message-wrapper message blocks
    - message-sender / message-date / message-text
    - dynamic message_id extraction from id="message###"
    - duplicate removal
    - timestamp parsing + drift detection
    - metadata preservation for timezone validation
    """

    def __init__(self, html_file: str, output_dir: str = None):
        self.html_file = Path(html_file)
        self.output_dir = Path(output_dir) if output_dir else self.html_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.log_file = self.output_dir / f"{self.html_file.stem}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self._setup_logging()

        self.stats = {
            "total_messages": 0,
            "duplicates_removed": 0,
            "timestamp_drifts": 0,
            "errors": 0,
        }

    def _setup_logging(self):
        self.logger = logging.getLogger(f"{self.html_file}_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        fh = logging.FileHandler(self.log_file, encoding="utf-8")
        fh.setFormatter(formatter)

        sh = logging.StreamHandler()
        sh.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(sh)

    # =========================
    # PARSE
    # =========================
    def parse_html(self) -> pd.DataFrame:
        with open(self.html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        metadata = self._extract_chat_metadata(soup)
        messages = self._find_message_elements(soup)

        rows = []
        for i, element in enumerate(messages, 1):
            try:
                row = self._extract_message_data(element, i, metadata)
                if row:
                    rows.append(row)
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning(f"Error parsing message {i}: {e}")

        self.stats["total_messages"] = len(rows)
        self.logger.info(f"Extracted {len(rows)} messages")
        return pd.DataFrame(rows)

    # =========================
    # METADATA
    # =========================
    def _extract_chat_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        metadata = {
            "conversation_participants": "",
            "participant_count": "",
            "local_user": "",
            "message_count": "",
            "conversation_first_timestamp": "",
            "conversation_last_timestamp": "",
            "case_time_zone": "",
        }

        try:
            for row in soup.find_all("tr"):
                label_cell = row.find("td", class_=re.compile(r"chat-label|chat-header", re.I))
                if not label_cell:
                    continue

                key = label_cell.get_text(" ", strip=True).lower()
                data_cells = row.find_all("td", class_="chat-data")
                if not data_cells:
                    continue

                if key == "display names":
                    nested_names = data_cells[0].find_all("div", class_="chat-data")
                    if nested_names:
                        values = [n.get_text(" ", strip=True) for n in nested_names if n.get_text(" ", strip=True)]
                    else:
                        values = [data_cells[0].get_text(" ", strip=True)] if data_cells[0].get_text(" ", strip=True) else []

                    if values:
                        metadata["conversation_participants"] = "; ".join(values)

                elif "number of participants" in key:
                    metadata["participant_count"] = data_cells[0].get_text(" ", strip=True)

                elif "local user" in key or "local participant" in key:
                    metadata["local_user"] = data_cells[0].get_text(" ", strip=True)

                elif "number of messages" in key:
                    metadata["message_count"] = data_cells[0].get_text(" ", strip=True)

                elif "first message sent" in key:
                    metadata["conversation_first_timestamp"] = data_cells[0].get_text(" ", strip=True)

                elif "last message sent" in key:
                    metadata["conversation_last_timestamp"] = data_cells[0].get_text(" ", strip=True)

                elif "case time zone" in key:
                    metadata["case_time_zone"] = data_cells[0].get_text(" ", strip=True)

        except Exception as e:
            self.logger.warning(f"Metadata extraction failed: {e}")

        return metadata

    # =========================
    # MESSAGE FINDER
    # =========================
    def _find_message_elements(self, soup: BeautifulSoup):
        messages = soup.find_all("label", class_="unknown-direction-message-wrapper")
        self.logger.info(f"Found {len(messages)} message elements")
        return messages

    # =========================
    # MESSAGE EXTRACTION
    # =========================
    def _extract_message_data(self, element, index: int, metadata: Dict[str, str]):
        message_id = self._extract_message_id(element)
        raw_timestamp = self._extract_raw_timestamp(element)
        parsed_timestamp = self._parse_timestamp(raw_timestamp)

        sender = element.find(class_="message-sender")
        text = element.find(class_="message-text")

        sender_text = sender.get_text(" ", strip=True) if sender else "Unknown"
        message_text = text.get_text(" ", strip=True) if text else ""

        if not any([message_id, raw_timestamp, sender_text, message_text]):
            return None

        return {
            "index": index,
            "message_id": message_id,
            "raw_timestamp": raw_timestamp if raw_timestamp else "",
            "timestamp": raw_timestamp if raw_timestamp else "",
            "parsed_timestamp": parsed_timestamp,
            "timestamp_parse_status": "OK" if parsed_timestamp is not None else "FAILED",
            "sender": sender_text,
            "recipient": metadata.get("conversation_participants", ""),
            "message": message_text,
            "conversation_participants": metadata.get("conversation_participants", ""),
            "participant_count": metadata.get("participant_count", ""),
            "local_user": metadata.get("local_user", ""),
            "message_count": metadata.get("message_count", ""),
            "conversation_first_timestamp": metadata.get("conversation_first_timestamp", ""),
            "conversation_last_timestamp": metadata.get("conversation_last_timestamp", ""),
            "case_time_zone": metadata.get("case_time_zone", ""),
            "source_file": self.html_file.name,
            "message_hash": self._generate_hash(message_id, raw_timestamp, sender_text, message_text),
        }

    def _extract_message_id(self, element) -> str:
        raw_id = element.get("id", "")
        if not raw_id:
            nested = element.find(attrs={"id": True})
            raw_id = nested.get("id", "") if nested else ""

        if not raw_id:
            return ""

        raw_id = raw_id.strip()
        match = re.search(r"message(\d+)", raw_id, re.I)
        if match:
            return match.group(1)

        return raw_id

    def _extract_raw_timestamp(self, element) -> str:
        found = element.find(class_="message-date")
        if found:
            return found.get_text(" ", strip=True)
        return ""

    def _parse_timestamp(self, raw_timestamp: str):
        if not raw_timestamp:
            return None
        try:
            return pd.to_datetime(raw_timestamp, errors="raise")
        except Exception:
            return None

    def _generate_hash(self, message_id: str, raw_timestamp: str, sender: str, message: str) -> str:
        raw = f"{message_id}|{raw_timestamp}|{sender}|{message}".encode("utf-8")
        return hashlib.md5(raw).hexdigest()

    # =========================
    # DUPES + DRIFT
    # =========================
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or "message_hash" not in df.columns:
            return df

        before = len(df)
        df = df.drop_duplicates(subset=["message_hash"], keep="first").reset_index(drop=True)
        removed = before - len(df)
        self.stats["duplicates_removed"] = removed
        self.logger.info(f"Removed {removed} duplicate messages")
        return df

    def check_timestamp_drift(self, df: pd.DataFrame, threshold_seconds: int = 300) -> pd.DataFrame:
        if df.empty:
            df["timestamp_drift_flag"] = False
            df["timestamp_drift_detail"] = ""
            df["timestamp_drift_seconds"] = ""
            return df

        df = df.copy()
        df["timestamp_drift_flag"] = False
        df["timestamp_drift_detail"] = ""
        df["timestamp_drift_seconds"] = ""

        previous_ts = None

        for idx in df.index:
            current_ts = df.at[idx, "parsed_timestamp"]

            if pd.isna(current_ts) or current_ts is None:
                df.at[idx, "timestamp_drift_flag"] = True
                df.at[idx, "timestamp_drift_detail"] = "Unparseable timestamp"
                df.at[idx, "timestamp_drift_seconds"] = ""
                self.stats["timestamp_drifts"] += 1
                continue

            if previous_ts is not None:
                delta = (current_ts - previous_ts).total_seconds()

                if delta < 0:
                    df.at[idx, "timestamp_drift_flag"] = True
                    df.at[idx, "timestamp_drift_detail"] = f"Time moved backward by {abs(int(delta))} seconds"
                    df.at[idx, "timestamp_drift_seconds"] = int(delta)
                    self.stats["timestamp_drifts"] += 1

                elif delta > threshold_seconds:
                    df.at[idx, "timestamp_drift_flag"] = True
                    df.at[idx, "timestamp_drift_detail"] = f"Forward jump greater than threshold ({int(delta)} seconds)"
                    df.at[idx, "timestamp_drift_seconds"] = int(delta)
                    self.stats["timestamp_drifts"] += 1

            previous_ts = current_ts

        self.logger.info(f"Detected {self.stats['timestamp_drifts']} timestamp drift issues")
        return df

    # =========================
    # SAVE
    # =========================
    def save_to_excel(self, df: pd.DataFrame, output_file: str = None) -> Path:
        if output_file:
            output_path = Path(output_file)
        else:
            output_path = self.output_dir / f"{self.html_file.stem}_parsed.xlsx"

        export_df = df.copy()

        if "parsed_timestamp" in export_df.columns:
            ts = pd.to_datetime(export_df["parsed_timestamp"], errors="coerce")
            export_df["parsed_timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

        if "timestamp" in export_df.columns:
            ts = pd.to_datetime(export_df["timestamp"], errors="coerce")
            export_df["timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna(export_df["timestamp"])

        preferred_columns = [
            "index",
            "message_id",
            "raw_timestamp",
            "timestamp",
            "parsed_timestamp",
            "timestamp_parse_status",
            "timestamp_drift_flag",
            "timestamp_drift_detail",
            "timestamp_drift_seconds",
            "sender",
            "recipient",
            "message",
            "conversation_participants",
            "participant_count",
            "local_user",
            "message_count",
            "conversation_first_timestamp",
            "conversation_last_timestamp",
            "case_time_zone",
            "source_file",
            "message_hash",
        ]

        existing_columns = [c for c in preferred_columns if c in export_df.columns]
        export_df = export_df[existing_columns]

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="Messages")

            summary = pd.DataFrame([
                {"Metric": "Source File", "Value": self.html_file.name},
                {"Metric": "Total Messages", "Value": self.stats["total_messages"]},
                {"Metric": "Duplicates Removed", "Value": self.stats["duplicates_removed"]},
                {"Metric": "Timestamp Drifts", "Value": self.stats["timestamp_drifts"]},
                {"Metric": "Errors", "Value": self.stats["errors"]},
            ])
            summary.to_excel(writer, index=False, sheet_name="Summary")

        self.logger.info(f"Saved: {output_path}")
        return output_path


# =========================
# SINGLE FILE
# =========================
def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:
    converter = TeamsChatConverter(html_file, output_dir)

    df = converter.parse_html()
    df = converter.remove_duplicates(df)
    df = converter.check_timestamp_drift(df)

    excel_file = converter.save_to_excel(df)
    return str(excel_file), str(converter.log_file)


# =========================
# FOLDER MODE
# =========================
def _iter_html_files(input_path: str, recursive: bool = False) -> List[Path]:
    p = Path(input_path).expanduser().resolve()

    if p.is_file():
        if p.suffix.lower() not in {".html", ".htm"}:
            raise ValueError(f"Input file is not HTML: {p}")
        return [p]

    if p.is_dir():
        patterns = ["**/*.html", "**/*.htm"] if recursive else ["*.html", "*.htm"]
        files = []
        for pattern in patterns:
            files.extend([f for f in p.glob(pattern) if f.is_file()])
        files = sorted(set(files))
        if not files:
            raise FileNotFoundError(f"No HTML files found in folder: {p}")
        return files

    raise FileNotFoundError(f"Input path not found: {p}")


def convert_teams_chat_folder(
    folder_path: str,
    output_dir: str = None,
    recursive: bool = False,
    combine: bool = True,
) -> Tuple[str, str]:
    html_files = _iter_html_files(folder_path, recursive=recursive)

    master = TeamsChatConverter(str(html_files[0]), output_dir)
    master.logger.info(f"Folder mode: {folder_path}")
    master.logger.info(f"Found {len(html_files)} HTML files")

    if not combine:
        last_excel, last_log = "", ""
        for f in html_files:
            last_excel, last_log = convert_teams_chat(str(f), output_dir=output_dir)
        return last_excel, last_log

    dfs = []
    for f in html_files:
        c = TeamsChatConverter(str(f), output_dir)
        df = c.parse_html()
        df = c.remove_duplicates(df)
        df = c.check_timestamp_drift(df)
        df["source_file"] = f.name
        dfs.append(df)

    if not dfs:
        raise ValueError("No messages extracted from any HTML file in the folder.")

    combined = pd.concat(dfs, ignore_index=True)

    if "message_hash" in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(subset=["message_hash"], keep="first")
        removed = before - len(combined)
        master.logger.info(f"Global duplicates removed: {removed}")

    combined.insert(0, "global_sequence", range(1, len(combined) + 1))

    out_dir = Path(output_dir).expanduser().resolve() if output_dir else Path(folder_path).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    combined_file = out_dir / f"teams_chats_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    export_df = combined.copy()

    if "parsed_timestamp" in export_df.columns:
        ts = pd.to_datetime(export_df["parsed_timestamp"], errors="coerce")
        export_df["parsed_timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

    if "timestamp" in export_df.columns:
        ts = pd.to_datetime(export_df["timestamp"], errors="coerce")
        export_df["timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna(export_df["timestamp"])

    preferred_columns = [
        "global_sequence",
        "index",
        "message_id",
        "raw_timestamp",
        "timestamp",
        "parsed_timestamp",
        "timestamp_parse_status",
        "timestamp_drift_flag",
        "timestamp_drift_detail",
        "timestamp_drift_seconds",
        "sender",
        "recipient",
        "message",
        "source_file",
        "message_hash",
    ]
    existing_columns = [c for c in preferred_columns if c in export_df.columns]
    export_df = export_df[existing_columns]

    with pd.ExcelWriter(combined_file, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="All Messages")

        summary = pd.DataFrame(
            [{"source_file": d["source_file"].iloc[0], "messages": len(d)} for d in dfs]
        )
        summary.to_excel(writer, index=False, sheet_name="Input Summary")

    master.logger.info(f"Saved combined workbook: {combined_file}")
    return str(combined_file), str(master.log_file)
