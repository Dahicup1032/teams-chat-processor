import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import List, Dict, Optional, Tuple
import hashlib
from urllib.parse import urlparse, unquote


class TeamsChatConverter:
    """Convert Microsoft Purview Teams HTML chat exports into structured Excel output."""

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
            "urls_extracted": 0,
            "attachments_found": 0,
            "messages_with_urls": 0,
            "messages_with_attachments": 0
        }

    def _setup_logging(self):
        self.logger = logging.getLogger(f"TeamsChatConverter_{self.html_file.stem}_{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        file_handler = logging.FileHandler(self.log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stream_handler)

    def parse_html(self) -> pd.DataFrame:
        """Parse HTML file and extract structured Teams chat data."""
        self.logger.info(f"Parsing HTML file: {self.html_file}")

        try:
            with open(self.html_file, "r", encoding="utf-8") as f:
                html_content = f.read()
        except Exception as e:
            self.logger.error(f"Error reading HTML file: {e}")
            raise

        soup = BeautifulSoup(html_content, "html.parser")

        chat_metadata = self._extract_chat_metadata(soup)
        message_elements = self._find_message_elements(soup)

        messages = []
        for idx, element in enumerate(message_elements, start=1):
            try:
                message_data = self._extract_message_data(element, idx, chat_metadata)
                if message_data:
                    messages.append(message_data)
            except Exception as e:
                self.logger.warning(f"Error parsing message {idx}: {e}")
                self.stats["errors"] += 1

        self.stats["total_messages"] = len(messages)
        self.logger.info(f"Extracted {len(messages)} messages from HTML")

        return pd.DataFrame(messages)

    def _extract_chat_metadata(self, soup: BeautifulSoup) -> Dict[str, str]:
        """
        Extract conversation-level metadata from Purview chat-data table.
        """
        metadata = {
            "conversation_participants": "",
            "participant_count": "",
            "local_user": "",
            "message_count": "",
            "conversation_first_timestamp": "",
            "conversation_last_timestamp": "",
            "case_time_zone": ""
        }

        try:
            rows = soup.find_all("tr")

            for row in rows:
                label_cell = row.find("td", class_=re.compile(r"chat-label|chat-header", re.I))
                if not label_cell:
                    continue

                key = label_cell.get_text(" ", strip=True).lower()
                data_cells = row.find_all("td", class_="chat-data")
                if not data_cells:
                    continue

                if "display names" in key:
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
       
    def _find_message_elements(self, soup: BeautifulSoup):
        """
        Find message wrapper elements.
        Prioritizes the newer Purview format first.
        """
        selectors = [
            ("label", {"class": "unknown-direction-message-wrapper"}),
            ("div", {"class": "message"}),
            ("div", {"class": "chat-message"}),
            ("div", {"class": "msg"}),
            ("tr", {"class": "message-row"}),
            ("div", {"data-type": "message"}),
            ("div", {"class": re.compile(r"MessageCard|message-card", re.I)}),
        ]

        for tag, attrs in selectors:
            elements = soup.find_all(tag, attrs)
            if elements:
                self.logger.info(f"Found {len(elements)} messages using selector: {tag} {attrs}")
                return elements

        fallback_elements = soup.find_all(
            lambda t: t.name in ["div", "label"] and (
                (t.get("class") and any(re.search(r"message|msg|chat", c, re.I) for c in t.get("class", [])))
                or (t.get("id") and re.search(r"message\d+", t.get("id", ""), re.I))
            )
        )

        if fallback_elements:
            self.logger.info(f"Found {len(fallback_elements)} messages using fallback pattern")
            return fallback_elements

        self.logger.warning("No messages found with standard selectors")
        return []

    def _extract_message_data(self, element, index: int, chat_metadata: Dict[str, str]) -> Optional[Dict]:
        """Extract one message row."""
        message_id = self._extract_message_id(element)
        timestamp = self._extract_timestamp(element)
        sender = self._extract_sender(element)
        message = self._extract_message_text(element)

        urls = self._extract_urls(element)
        attachments = self._extract_attachments(element)

        if urls:
            self.stats["urls_extracted"] += len(urls)
            self.stats["messages_with_urls"] += 1

        if attachments:
            self.stats["attachments_found"] += len(attachments)
            self.stats["messages_with_attachments"] += 1

        if not any([message, sender, timestamp, message_id]):
            return None

        return {
            "msg_index": index,
            "message_id": message_id,
            "timestamp": timestamp,
            "sender": sender,
            "recipient": chat_metadata.get("conversation_participants", ""),
            "message": message,
            "conversation_participants": chat_metadata.get("conversation_participants", ""),
            "participant_count": chat_metadata.get("participant_count", ""),
            "local_user": chat_metadata.get("local_user", ""),
            "message_count": chat_metadata.get("message_count", ""),
            "conversation_first_timestamp": chat_metadata.get("conversation_first_timestamp", ""),
            "conversation_last_timestamp": chat_metadata.get("conversation_last_timestamp", ""),
            "case_time_zone": chat_metadata.get("case_time_zone", ""),
            "source_file": str(self.html_file.name),
            "urls": self._format_urls_list(urls),
            "url_count": len(urls),
            "attachments": self._format_attachments_list(attachments),
            "attachment_count": len(attachments),
            "has_urls": len(urls) > 0,
            "has_attachments": len(attachments) > 0,
            "message_hash": self._generate_hash(message_id, timestamp, sender, message)
        }

    def _extract_message_id(self, element) -> str:
        """
        Extract numeric message ID from id values like:
        id="message339" -> 339

        Nothing is hardcoded. Any numeric sequence after 'message' is extracted.
        """
        candidates = [element]

        nested_with_id = element.find(attrs={"id": True})
        if nested_with_id:
            candidates.append(nested_with_id)

        for candidate in candidates:
            raw_id = candidate.get("id", "")
            if not raw_id:
                continue

            raw_id = raw_id.strip()
            match = re.search(r"message(\d+)", raw_id, re.I)
            if match:
                return match.group(1)

            return raw_id

        return ""

    def _extract_timestamp(self, element) -> str:
        """Extract timestamp from new and legacy patterns."""
        date_elem = element.find(class_="message-date")
        if date_elem:
            return date_elem.get_text(" ", strip=True)

        patterns = [
            ("time", {}),
            ("span", {"class": re.compile(r"time|date|timestamp", re.I)}),
            ("div", {"class": re.compile(r"time|date|timestamp", re.I)}),
        ]

        for tag, attrs in patterns:
            found = element.find(tag, attrs)
            if found:
                text = found.get_text(" ", strip=True)
                if text:
                    return text

        for attr in ["datetime", "data-timestamp", "data-time"]:
            if element.get(attr):
                return str(element.get(attr)).strip()

        return ""

    def _extract_sender(self, element) -> str:
        """Extract sender from new and legacy patterns."""
        sender_elem = element.find(class_="message-sender")
        if sender_elem:
            sender = sender_elem.get_text(" ", strip=True)
            if sender:
                return sender

        patterns = [
            ("span", {"class": re.compile(r"sender|from|author|name", re.I)}),
            ("div", {"class": re.compile(r"sender|from|author|name", re.I)}),
            ("strong", {}),
            ("b", {}),
        ]

        for tag, attrs in patterns:
            found = element.find(tag, attrs)
            if found:
                text = found.get_text(" ", strip=True)
                if text and len(text) < 200:
                    return text

        return "Unknown"

    def _extract_message_text(self, element) -> str:
        """
        Extract full visible message text.
        message-text may include normal text and inline URLs.
        """
        text_elem = element.find(class_="message-text")
        if text_elem:
            return text_elem.get_text(" ", strip=True)

        patterns = [
            ("div", {"class": re.compile(r"message-content|msg-content|content|body|text", re.I)}),
            ("p", {"class": re.compile(r"message|msg|text", re.I)}),
            ("span", {"class": re.compile(r"message|msg|text", re.I)}),
        ]

        for tag, attrs in patterns:
            found = element.find(tag, attrs)
            if found:
                text = found.get_text(" ", strip=True)
                if text:
                    return text

        text = element.get_text(" ", strip=True)
        return text if text else ""

    def _extract_urls(self, element) -> List[Dict]:
        """Extract URLs from anchor tags and raw text."""
        urls = []
        seen = set()

        for link in element.find_all("a", href=True):
            url = link.get("href", "").strip()
            display_text = link.get_text(" ", strip=True)

            if not url or url in seen:
                continue
            if url.startswith(("javascript:", "mailto:", "#", "tel:")):
                continue

            urls.append({
                "url": url,
                "text": display_text if display_text else url,
                "type": self._classify_url(url)
            })
            seen.add(url)

        text_content = element.get_text(" ", strip=True)
        url_pattern = r"http[s]?://(?:[a-zA-Z0-9$\-_.+!*'(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"

        for match in re.finditer(url_pattern, text_content):
            url = match.group(0)
            if url not in seen:
                urls.append({
                    "url": url,
                    "text": url,
                    "type": self._classify_url(url)
                })
                seen.add(url)

        return urls

    def _classify_url(self, url: str) -> str:
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc
            path = parsed.path

            if "sharepoint.com" in domain or "sharepoint." in domain:
                return "SharePoint"
            if "teams.microsoft.com" in domain or "teams.live.com" in domain:
                return "Teams"
            if "onedrive" in domain:
                return "OneDrive"
            if any(d in domain for d in ["dropbox.com", "box.com", "drive.google.com"]):
                return "File Sharing"
            if any(d in domain for d in ["zoom.us", "meet.google.com", "webex.com"]):
                return "Meeting"
            if any(path.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"]):
                return "Document"
            if any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mp3"]):
                return "Media"
            return "Web"
        except Exception:
            return "Unknown"

    def _extract_attachments(self, element) -> List[Dict]:
        """Extract attachment-like elements."""
        attachments = []

        patterns = [
            ("div", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("span", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("a", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("li", {"class": re.compile(r"attachment|file|document", re.I)}),
        ]

        for tag, attrs in patterns:
            for found in element.find_all(tag, attrs):
                att = self._parse_attachment_element(found)
                if att and att not in attachments:
                    attachments.append(att)

        for link in element.find_all("a", href=True):
            href = link.get("href", "")
            if (
                "download" in href.lower()
                or any(ext in href.lower() for ext in [
                    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
                    ".zip", ".rar", ".7z", ".txt", ".csv", ".jpg", ".jpeg", ".png", ".gif", ".mp4"
                ])
            ):
                att = {
                    "filename": self._extract_filename_from_url(href),
                    "url": href,
                    "size": "Unknown",
                    "type": self._get_file_type(href)
                }
                if att not in attachments:
                    attachments.append(att)

        return attachments

    def _parse_attachment_element(self, element) -> Optional[Dict]:
        try:
            filename = None

            for attr in ["title", "data-filename", "data-file", "aria-label"]:
                if element.get(attr):
                    filename = element.get(attr)
                    break

            if not filename:
                text = element.get_text(" ", strip=True)
                if text and 0 < len(text) < 200:
                    filename = text

            url = None
            if element.name == "a" and element.get("href"):
                url = element.get("href")
            else:
                link = element.find("a", href=True)
                if link:
                    url = link.get("href")

            size = "Unknown"
            size_elem = element.find(["span", "div"], class_=re.compile(r"size|filesize", re.I))
            if size_elem:
                size = size_elem.get_text(" ", strip=True)

            if filename or url:
                return {
                    "filename": filename or "Unknown",
                    "url": url or "",
                    "size": size,
                    "type": self._get_file_type(filename or url or "")
                }
        except Exception:
            return None

        return None

    def _extract_filename_from_url(self, url: str) -> str:
        try:
            path = urlparse(url).path
            filename = unquote(path.split("/")[-1])
            if filename and "." in filename:
                return filename
        except Exception:
            pass
        return "Unknown"

    def _get_file_type(self, filename: str) -> str:
        filename_lower = filename.lower()

        extensions = {
            "Document": [".pdf", ".doc", ".docx", ".txt", ".rtf", ".odt"],
            "Spreadsheet": [".xls", ".xlsx", ".csv", ".ods"],
            "Presentation": [".ppt", ".pptx", ".odp"],
            "Image": [".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp"],
            "Video": [".mp4", ".avi", ".mov", ".wmv", ".flv", ".mkv"],
            "Audio": [".mp3", ".wav", ".ogg", ".m4a", ".flac"],
            "Archive": [".zip", ".rar", ".7z", ".tar", ".gz"],
            "Code": [".py", ".js", ".html", ".css", ".java", ".cpp", ".c", ".h"],
        }

        for file_type, exts in extensions.items():
            if any(filename_lower.endswith(ext) for ext in exts):
                return file_type
        return "Other"

    def _format_urls_list(self, urls: List[Dict]) -> str:
        if not urls:
            return ""

        lines = []
        for i, url_info in enumerate(urls, start=1):
            url = url_info["url"]
            text = url_info.get("text", url)
            url_type = url_info.get("type", "Web")
            lines.append(f"[{i}] {text} ({url_type}): {url}")
        return "\n".join(lines)

    def _format_attachments_list(self, attachments: List[Dict]) -> str:
        if not attachments:
            return ""

        lines = []
        for i, att in enumerate(attachments, start=1):
            filename = att.get("filename", "Unknown")
            file_type = att.get("type", "Unknown")
            size = att.get("size", "Unknown")
            url = att.get("url", "")

            line = f"[{i}] {filename} ({file_type}, {size})"
            if url:
                line += f" - {url}"
            lines.append(line)

        return "\n".join(lines)

    def _generate_hash(self, message_id: str, timestamp: str, sender: str, message: str) -> str:
        content = f"{message_id}|{timestamp}|{sender}|{message}".encode("utf-8")
        return hashlib.md5(content).hexdigest()

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate messages using message_hash."""
        self.logger.info("Checking for duplicates...")
        if df.empty or "message_hash" not in df.columns:
            return df

        initial_count = len(df)
        df = df.drop_duplicates(subset=["message_hash"], keep="first")
        final_count = len(df)

        removed = initial_count - final_count
        self.stats["duplicates_removed"] = removed

        if removed > 0:
            self.logger.info(f"Removed {removed} duplicate messages")
        else:
            self.logger.info("No duplicate messages found")

        return df.reset_index(drop=True)

    def check_timestamp_drift(self, df: pd.DataFrame, threshold_seconds: int = 300) -> pd.DataFrame:
        """
        Check for possible timestamp drift.

        Flags:
        - unparseable timestamps
        - backward time movement
        - unusually large forward jumps beyond threshold_seconds
        """
        self.logger.info("Checking timestamp drift...")

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

        previous_ts = None

        for idx in df.index:
            current_ts = df.at[idx, "parsed_timestamp"]
            raw_ts = df.at[idx, "timestamp"] if "timestamp" in df.columns else ""

            if pd.isna(current_ts):
                df.at[idx, "timestamp_drift_flag"] = True
                df.at[idx, "timestamp_drift_detail"] = "Unparseable timestamp"
                df.at[idx, "timestamp_drift_seconds"] = ""
                self.stats["timestamp_drifts"] += 1
                self.logger.warning(f"Row {idx}: unparseable timestamp -> {raw_ts}")
                continue

            if previous_ts is not None:
                delta = (current_ts - previous_ts).total_seconds()

                if delta < 0:
                    df.at[idx, "timestamp_drift_flag"] = True
                    df.at[idx, "timestamp_drift_detail"] = f"Time moved backward by {abs(int(delta))} seconds"
                    df.at[idx, "timestamp_drift_seconds"] = int(delta)
                    self.stats["timestamp_drifts"] += 1
                    self.logger.warning(
                        f"Row {idx}: timestamp moved backward. previous={previous_ts}, current={current_ts}, delta={int(delta)}"
                    )

                elif delta > threshold_seconds:
                    df.at[idx, "timestamp_drift_flag"] = True
                    df.at[idx, "timestamp_drift_detail"] = f"Forward jump greater than threshold ({int(delta)} seconds)"
                    df.at[idx, "timestamp_drift_seconds"] = int(delta)
                    self.stats["timestamp_drifts"] += 1
                    self.logger.warning(
                        f"Row {idx}: large timestamp jump. previous={previous_ts}, current={current_ts}, delta={int(delta)}"
                    )

            previous_ts = current_ts

        self.logger.info(f"Timestamp drift check complete. Detected {self.stats['timestamp_drifts']} drift issues.")
        return df

    def save_to_excel(self, df: pd.DataFrame, output_file: str = None) -> Path:
        """Save parsed messages to Excel."""
        if output_file:
            output_path = Path(output_file)
        else:
            output_path = self.output_dir / f"{self.html_file.stem}_parsed.xlsx"

        preferred_columns = [
            "msg_index",
            "message_id",
            "timestamp",
            "parsed_timestamp",
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
            "urls",
            "url_count",
            "attachments",
            "attachment_count",
            "has_urls",
            "has_attachments",
            "message_hash",
        ]

        existing_columns = [c for c in preferred_columns if c in df.columns]
        df_to_save = df[existing_columns].copy()

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df_to_save.to_excel(writer, index=False, sheet_name="Messages")

            summary = pd.DataFrame([
                {"Metric": "Source File", "Value": self.html_file.name},
                {"Metric": "Total Messages", "Value": self.stats["total_messages"]},
                {"Metric": "Duplicates Removed", "Value": self.stats["duplicates_removed"]},
                {"Metric": "Timestamp Drifts", "Value": self.stats["timestamp_drifts"]},
                {"Metric": "URLs Extracted", "Value": self.stats["urls_extracted"]},
                {"Metric": "Attachments Found", "Value": self.stats["attachments_found"]},
                {"Metric": "Messages With URLs", "Value": self.stats["messages_with_urls"]},
                {"Metric": "Messages With Attachments", "Value": self.stats["messages_with_attachments"]},
                {"Metric": "Errors", "Value": self.stats["errors"]},
            ])
            summary.to_excel(writer, index=False, sheet_name="Summary")

        self.logger.info(f"Saved Excel output to: {output_path}")
        return output_path


def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:
    """
    Convert a Teams HTML export to Excel.

    Returns:
        (excel_output_path, log_output_path)
    """
    converter = TeamsChatConverter(html_file, output_dir=output_dir)

    df = converter.parse_html()

    if df.empty:
        raise ValueError("No messages were extracted from the HTML file.")

    df = converter.remove_duplicates(df)
    df = converter.check_timestamp_drift(df)
    excel_path = converter.save_to_excel(df)

    return str(excel_path), str(converter.log_file)
    
