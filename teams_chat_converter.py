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

    Built for exports that contain:
    - metadata rows with chat-label / chat-data
    - message wrappers:
        - sent-message-wrapper
        - received-message-wrapper
        - unknown-direction-message-wrapper
    - message fields:
        - message-sender
        - message-date
        - message-text
    - message ids like id="message1234"
    """

    MESSAGE_WRAPPER_SELECTOR = (
        "label.sent-message-wrapper, "
        "label.received-message-wrapper, "
        "label.unknown-direction-message-wrapper"
    )

    def __init__(self, html_file: str, output_dir: str = None):
        self.html_file = Path(html_file)
        self.output_dir = Path(output_dir) if output_dir else self.html_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.log_file = self.output_dir / (
            f"{self.html_file.stem}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
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
        with open(self.html_file, "r", encoding="utf-8", errors="ignore") as f:
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
        self.logger.info(f"Extracted {len(rows)} messages from {self.html_file.name}")
        return pd.DataFrame(rows)

    # =========================
    # METADATA
    # =========================
    def _extract_chat_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        metadata = {
            "conversation_title": "",
            "conversation_participants": "",
            "participant_count": "",
            "local_user": "",
            "message_count": "",
            "conversation_first_timestamp": "",
            "conversation_last_timestamp": "",
            "case_time_zone": "",
        }

        try:
            title = soup.find(class_=re.compile(r"chat-title", re.I))
            if title:
                metadata["conversation_title"] = title.get_text(" ", strip=True)

            for row in soup.find_all("tr"):
                label_cell = row.find("td", class_=re.compile(r"chat-label|chat-header", re.I))
                if not label_cell:
                    continue

                key_raw = label_cell.get_text(" ", strip=True)
                key = self._normalize_label(key_raw)

                data_cells = row.find_all("td", class_=re.compile(r"chat-data", re.I))
                if not data_cells:
                    continue

                if key == "display_names":
                    values = self._extract_display_names(data_cells[0])
                    metadata["conversation_participants"] = "; ".join(values)

                elif key == "number_of_participants":
                    metadata["participant_count"] = data_cells[0].get_text(" ", strip=True)

                elif key == "local_user":
                    metadata["local_user"] = data_cells[0].get_text(" ", strip=True)

                elif key == "number_of_messages":
                    metadata["message_count"] = data_cells[0].get_text(" ", strip=True)

                elif key == "first_message_sent":
                    metadata["conversation_first_timestamp"] = data_cells[0].get_text(" ", strip=True)

                elif key == "last_message_sent":
                    metadata["conversation_last_timestamp"] = data_cells[0].get_text(" ", strip=True)

                elif key == "case_time_zone":
                    metadata["case_time_zone"] = data_cells[0].get_text(" ", strip=True)

        except Exception as e:
            self.logger.warning(f"Metadata extraction failed: {e}")

        return metadata

    def _normalize_label(self, text: str) -> str:
        if not text:
            return ""

        key = re.sub(r"\s+", " ", text.strip().lower())

        mapping = {
            "display names": "display_names",
            "number of participants": "number_of_participants",
            "local user": "local_user",
            "local participant": "local_user",
            "number of messages": "number_of_messages",
            "first message sent": "first_message_sent",
            "first message sent date/time": "first_message_sent",
            "last message sent": "last_message_sent",
            "last message sent date/time": "last_message_sent",
            "case time zone": "case_time_zone",
        }
        return mapping.get(key, key.replace(" ", "_"))

    def _extract_display_names(self, cell) -> List[str]:
        nested_names = cell.find_all("div", class_=re.compile(r"chat-data", re.I))
        if nested_names:
            values = [n.get_text(" ", strip=True) for n in nested_names if n.get_text(" ", strip=True)]
            return values

        text = cell.get_text(" ", strip=True)
        if not text:
            return []

        if ";" in text:
            return [x.strip() for x in text.split(";") if x.strip()]
        if "," in text:
            return [x.strip() for x in text.split(",") if x.strip()]
        return [text]

    # =========================
    # MESSAGE FINDER
    # =========================
    def _find_message_elements(self, soup: BeautifulSoup):
        threaded_chat = soup.select_one("div.threaded-chat") or soup.select_one("div#container")
        scope = threaded_chat if threaded_chat else soup

        messages = scope.select(self.MESSAGE_WRAPPER_SELECTOR)
        self.logger.info(f"Found {len(messages)} message elements")
        return messages

    # =========================
    # MESSAGE EXTRACTION
    # =========================
    def _extract_message_data(self, element, index: int, metadata: Dict[str, str]):
        message_id = self._extract_message_id(element)
        raw_timestamp = self._extract_raw_timestamp(element)
        parsed_timestamp = self._parse_timestamp(raw_timestamp)

        sender_text = self._extract_sender(element)
        message_text = self._extract_message_text(element)

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
            "recipients": metadata.get("conversation_participants", ""),
            "message": message_text,
            "conversation_title": metadata.get("conversation_title", ""),
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
        raw_id = (element.get("id") or "").strip()

        if raw_id:
            match = re.search(r"message(\d+)", raw_id, re.I)
            if match:
                return match.group(1)
            return raw_id

        checkbox = element.find("input", attrs={"value": True})
        if checkbox:
            value = (checkbox.get("value") or "").strip()
            if value:
                return value

        nested = element.find(attrs={"id": True})
        if nested:
            nested_id = (nested.get("id") or "").strip()
            match = re.search(r"message(\d+)", nested_id, re.I)
            if match:
                return match.group(1)
            return nested_id

        return ""

    def _extract_sender(self, element) -> str:
        found = element.find(class_=re.compile(r"message-sender", re.I))
        if found:
            return found.get_text(" ", strip=True)
        return "Unknown"

    def _extract_raw_timestamp(self, element) -> str:
        found = element.find(class_=re.compile(r"message-date", re.I))
        if found:
            return found.get_text(" ", strip=True)
        return ""

    def _extract_message_text(self, element) -> str:
        found = element.find(class_=re.compile(r"message-text", re.I))
        if found:
            return found.get_text(" ", strip=True)

        bubble = element.find(class_=re.compile(r"(sent|received|unknown-direction)-message", re.I))
        if bubble:
            return bubble.get_text(" ", strip=True)

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
        if df.empty or "parsed_timestamp" not in df.columns:
            return df

        df = df.copy()
        df["timestamp_drift_flag"] = ""
        df["timestamp_drift_detail"] = ""
        df["timestamp_drift_seconds"] = ""

        parsed = pd.to_datetime(df["parsed_timestamp"], errors="coerce")
        valid_idx = parsed.dropna().index.tolist()

        for i in range(1, len(valid_idx)):
            prev_idx = valid_idx[i - 1]
            curr_idx = valid_idx[i]

            prev_ts = parsed.loc[prev_idx]
            curr_ts = parsed.loc[curr_idx]

            diff = (curr_ts - prev_ts).total_seconds()
            if abs(diff) > threshold_seconds:
                df.loc[curr_idx, "timestamp_drift_flag"] = "YES"
                df.loc[curr_idx, "timestamp_drift_detail"] = (
                    f"Gap from previous parsed timestamp exceeds {threshold_seconds} seconds"
                )
                df.loc[curr_idx, "timestamp_drift_seconds"] = int(diff)
                self.stats["timestamp_drifts"] += 1

        return df

    # =========================
    # END-TO-END
    # =========================
    def convert(self) -> Tuple[str, str]:
        df = self.parse_html()
        df = self.remove_duplicates(df)
        df = self.check_timestamp_drift(df)

        output_file = self.output_dir / f"{self.html_file.stem}_converted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        export_df = df.copy()

        if "parsed_timestamp" in export_df.columns:
            ts = pd.to_datetime(export_df["parsed_timestamp"], errors="coerce")
            export_df["parsed_timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

        if "timestamp" in export_df.columns:
            ts = pd.to_datetime(export_df["timestamp"], errors="coerce")
            export_df["timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S").fillna(export_df["timestamp"])

        preferred_columns = [
            "sender",
            "recipients",
            "message",
            "timestamp",
            "message_id",
            "source_file",
            "parsed_timestamp",
            "timestamp_parse_status",
            "timestamp_drift_flag",
            "timestamp_drift_detail",
            "timestamp_drift_seconds",
            "message_hash",
            "conversation_title",
            "conversation_participants",
            "participant_count",
            "local_user",
            "message_count",
            "conversation_first_timestamp",
            "conversation_last_timestamp",
            "case_time_zone",
            "index",
            "raw_timestamp",
        ]
        existing_columns = [c for c in preferred_columns if c in export_df.columns]
        export_df = export_df[existing_columns]

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            export_df.to_excel(writer, index=False, sheet_name="Messages")

            stats_df = pd.DataFrame(
                [
                    {"metric": "source_file", "value": self.html_file.name},
                    {"metric": "total_messages", "value": self.stats["total_messages"]},
                    {"metric": "duplicates_removed", "value": self.stats["duplicates_removed"]},
                    {"metric": "timestamp_drifts", "value": self.stats["timestamp_drifts"]},
                    {"metric": "errors", "value": self.stats["errors"]},
                ]
            )
            stats_df.to_excel(writer, index=False, sheet_name="Stats")

        self.logger.info(f"Saved workbook: {output_file}")
        return str(output_file), str(self.log_file)


def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:
    converter = TeamsChatConverter(html_file, output_dir)
    return converter.convert()


def _iter_html_files(path_str: str, recursive: bool = False) -> List[Path]:
    p = Path(path_str).expanduser().resolve()

    if p.is_file():
        if p.suffix.lower() != ".html":
            raise FileNotFoundError(f"File is not an HTML file: {p}")
        return [p]

    if p.is_dir():
        if recursive:
            files = sorted([x for x in p.rglob("*.html") if x.is_file()])
        else:
            files = sorted([x for x in p.glob("*.html") if x.is_file()])

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
        combined = combined.drop_duplicates(subset=["message_hash"], keep="first").reset_index(drop=True)
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
        "sender",
        "recipients",
        "message",
        "timestamp",
        "message_id",
        "source_file",
        "parsed_timestamp",
        "timestamp_parse_status",
        "timestamp_drift_flag",
        "timestamp_drift_detail",
        "timestamp_drift_seconds",
        "message_hash",
        "conversation_title",
        "conversation_participants",
        "participant_count",
        "local_user",
        "message_count",
        "conversation_first_timestamp",
        "conversation_last_timestamp",
        "case_time_zone",
        "index",
        "raw_timestamp",
    ]
    existing_columns = [c for c in preferred_columns if c in export_df.columns]
    export_df = export_df[existing_columns]

    with pd.ExcelWriter(combined_file, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="All Messages")

        summary = pd.DataFrame(
            [
                {
                    "source_file": d["source_file"].iloc[0] if (d is not None and not d.empty and "source_file" in d.columns) else "empty_or_failed_parse",
                    "messages": len(d) if d is not None else 0,
                }
                for d in dfs if d is not None
            ]
        )
        summary.to_excel(writer, index=False, sheet_name="Input Summary")

    master.logger.info(f"Saved combined workbook: {combined_file}")
    return str(combined_file), str(master.log_file)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Convert Purview Teams HTML chat exports to Excel")
    parser.add_argument("input_path", help="Path to HTML file or folder of HTML files")
    parser.add_argument("-o", "--output-dir", default=None, help="Output directory")
    parser.add_argument("-r", "--recursive", action="store_true", help="Search subfolders for HTML files")
    parser.add_argument("--no-combine", action="store_true", help="Do not combine folder results into one workbook")

    args = parser.parse_args()

    if Path(args.input_path).is_dir():
        excel_file, log_file = convert_teams_chat_folder(
            args.input_path,
            output_dir=args.output_dir,
            recursive=args.recursive,
            combine=not args.no_combine,
        )
    else:
        excel_file, log_file = convert_teams_chat(args.input_path, output_dir=args.output_dir)

    print(f"Excel Output: {excel_file}")
<<<<<<< Updated upstream
    print(f"Log File: {log_file}")
=======
    print(f"Log File: {log_file}")
>>>>>>> Stashed changes
