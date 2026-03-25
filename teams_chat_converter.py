import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import Tuple, List
import hashlib


class TeamsChatConverter:
    """
    Stable Purview Teams HTML parser.

    Supports:
    - Display names -> recipient/group context
    - unknown-direction-message-wrapper message blocks
    - message-sender
    - message-date
    - message-text
    - dynamic message_id extraction from id="message###"
    - duplicate removal
    - timestamp drift detection
    - Excel output
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
        self.logger = logging.getLogger(str(self.html_file))
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        fh = logging.FileHandler(self.log_file, encoding="utf-8")
        fh.setFormatter(formatter)

        sh = logging.StreamHandler()
        sh.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(sh)

    def parse_html(self) -> pd.DataFrame:
        with open(self.html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        participants = self._extract_participants(soup)
        messages = soup.find_all("label", class_="unknown-direction-message-wrapper")

        self.logger.info(f"Found {len(messages)} message elements")

        rows = []
        for i, element in enumerate(messages, 1):
            try:
                row = self._extract_message(element, i, participants)
                if row:
                    rows.append(row)
            except Exception as e:
                self.stats["errors"] += 1
                self.logger.warning(f"Error parsing message {i}: {e}")

        self.stats["total_messages"] = len(rows)
        self.logger.info(f"Extracted {len(rows)} messages")
        return pd.DataFrame(rows)

    def _extract_participants(self, soup) -> str:
        """
        Extract only participant names from the 'Display names' row.
        """
        for row in soup.find_all("tr"):
            label = row.find("td", class_="chat-label")
            if not label:
                continue

            if label.get_text(" ", strip=True).lower() != "display names":
                continue

            data = row.find("td", class_="chat-data")
            if not data:
                return ""

            nested = data.find_all("div", class_="chat-data")
            if nested:
                names = [n.get_text(" ", strip=True) for n in nested if n.get_text(" ", strip=True)]
                return "; ".join(names)

            text = data.get_text(" ", strip=True)
            return text if text else ""

        return ""

    def _extract_message_id(self, element) -> str:
        """
        Extract numeric id from id="message339" -> "339"
        Fully dynamic, nothing hardcoded.
        """
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

    def _extract_message(self, element, idx: int, participants: str):
        sender = element.find(class_="message-sender")
        text = element.find(class_="message-text")
        time = element.find(class_="message-date")

        sender_text = sender.get_text(" ", strip=True) if sender else "Unknown"
        message_text = text.get_text(" ", strip=True) if text else ""
        timestamp_text = time.get_text(" ", strip=True) if time else ""
        message_id = self._extract_message_id(element)

        if not any([sender_text, message_text, timestamp_text, message_id]):
            return None

        return {
            "index": idx,
            "message_id": message_id,
            "timestamp": timestamp_text,
            "sender": sender_text,
            "recipient": participants,
            "message": message_text,
            "message_hash": self._hash(message_id, timestamp_text, sender_text, message_text),
        }

    def _hash(self, message_id: str, timestamp: str, sender: str, message: str) -> str:
        raw = f"{message_id}|{timestamp}|{sender}|{message}".encode("utf-8")
        return hashlib.md5(raw).hexdigest()

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
        """
        Flag:
        - unparseable timestamps
        - backward movement
        - unusually large jumps
        """
        if df.empty:
            df["parsed_timestamp"] = pd.NaT
            df["timestamp_drift_flag"] = False
            df["timestamp_drift_detail"] = ""
            df["timestamp_drift_seconds"] = ""
            return df

        df = df.copy()
        df["parsed_timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df["timestamp_drift_flag"] = False
        df["timestamp_drift_detail"] = ""
        df["timestamp_drift_seconds"] = ""

        prev = None

        for idx in df.index:
            current = df.at[idx, "parsed_timestamp"]
            raw = df.at[idx, "timestamp"]

            if pd.isna(current):
                df.at[idx, "timestamp_drift_flag"] = True
                df.at[idx, "timestamp_drift_detail"] = "Unparseable timestamp"
                self.stats["timestamp_drifts"] += 1
                self.logger.warning(f"Row {idx}: unparseable timestamp -> {raw}")
                continue

            if prev is not None:
                delta = (current - prev).total_seconds()

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

            prev = current

        self.logger.info(f"Detected {self.stats['timestamp_drifts']} timestamp drift issues")
        return df

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
            "timestamp",
            "parsed_timestamp",
            "timestamp_drift_flag",
            "timestamp_drift_detail",
            "timestamp_drift_seconds",
            "sender",
            "recipient",
            "message",
            "message_hash",
        ]

        existing = [c for c in preferred_columns if c in export_df.columns]
        export_df = export_df[existing]

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


def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:
    converter = TeamsChatConverter(html_file, output_dir)
    df = converter.parse_html()
    df = converter.remove_duplicates(df)
    df = converter.check_timestamp_drift(df)
    excel_file = converter.save_to_excel(df)
    return str(excel_file), str(converter.log_file)


def _iter_html_files(input_path: str, recursive: bool = False) -> List[Path]:
    p = Path(input_path).expanduser().resolve()

    if p.is_file():
        if p.suffix.lower() not in {".html", ".htm"}:
            raise ValueError(f"Input file is not HTML: {p}")
        return [p]

    if p.is_dir():
        pattern = "**/*.html" if recursive else "*.html"
        files = sorted([f for f in p.glob(pattern) if f.is_file()])
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
        "timestamp",
        "parsed_timestamp",
        "timestamp_drift_flag",
        "timestamp_drift_detail",
        "timestamp_drift_seconds",
        "sender",
        "recipient",
        "message",
        "source_file",
        "message_hash",
    ]
    existing = [c for c in preferred_columns if c in export_df.columns]
    export_df = export_df[existing]

    with pd.ExcelWriter(combined_file, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="All Messages")

        summary = pd.DataFrame(
            [{"source_file": d["source_file"].iloc[0], "messages": len(d)} for d in dfs]
        )
        summary.to_excel(writer, index=False, sheet_name="Input Summary")

    master.logger.info(f"Saved combined workbook: {combined_file}")
    return str(combined_file), str(master.log_file)
