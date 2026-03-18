import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import List, Dict, Optional
import hashlib
from urllib.parse import urlparse, unquote


class TeamsChatConverter:
    """Convert Purview Teams HTML chat exports to Excel-friendly structured data."""

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
            "processing_time": 0,
            "urls_extracted": 0,
            "attachments_found": 0,
            "messages_with_urls": 0,
            "messages_with_attachments": 0
        }

    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(self.log_file, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def parse_html(self) -> pd.DataFrame:
        """Parse HTML file and extract structured Teams chat messages."""
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
        Extract conversation-level metadata from the top chat-data table.
        Expected patterns include rows like:
        CHAT PARTICIPANTS
        DISPLAY NAMES
        LOCAL PARTICIPANT
        NUMBER OF MESSAGES
        FIRST MESSAGE SENT
        LAST MESSAGE SENT
        CASE TIME ZONE
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
                header = row.find("th")
                if not header:
                    continue

                key = header.get_text(" ", strip=True).lower()

                # collect all chat-data cells in this row
                cells = row.find_all("td", class_="chat-data")
                cell_values = [c.get_text(" ", strip=True) for c in cells if c.get_text(" ", strip=True)]

                if not cell_values:
                    continue

                if "display names" in key:
                    metadata["conversation_participants"] = "; ".join(cell_values)

                elif "chat participants" in key and "display" not in key:
                    metadata["participant_count"] = cell_values[0]

                elif "local participant" in key:
                    metadata["local_user"] = cell_values[0]

                elif "number of messages" in key:
                    metadata["message_count"] = cell_values[0]

                elif "first message" in key:
                    metadata["conversation_first_timestamp"] = cell_values[0]

                elif "last message" in key:
                    metadata["conversation_last_timestamp"] = cell_values[0]

                elif "time zone" in key:
                    metadata["case_time_zone"] = cell_values[0]

        except Exception as e:
            self.logger.warning(f"Metadata extraction failed: {e}")

        return metadata

    def _find_message_elements(self, soup: BeautifulSoup):
        """
        Find message wrapper elements.
        Prioritizes the new Purview format first.
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

        elements = soup.find_all(
            lambda t: t.name in ["div", "label"] and (
                (t.get("class") and any(re.search(r"message|msg|chat", c, re.I) for c in t.get("class", [])))
                or (t.get("id") and re.search(r"message\d+", t.get("id", ""), re.I))
            )
        )

        if elements:
            self.logger.info(f"Found {len(elements)} messages using fallback pattern")
            return elements

        self.logger.warning("No messages found with standard selectors")
        return []

    def _extract_message_data(self, element, index: int, chat_metadata: Dict[str, str]) -> Optional[Dict]:
        """Extract a single message row."""
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

        # keep row if there is actual message content
        if message:
            return {
                "msg_index": index,
                "message_id": message_id,
                "timestamp": timestamp,
                "sender": sender,
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
                "message_hash": self._generate_hash(
                    message_id=message_id,
                    timestamp=timestamp,
                    sender=sender,
                    message=message
                )
            }

        return None

    def _extract_message_id(self, element) -> str:
        """
        Extract message ID.
        Expected real-world pattern:
            id="message339"  -> returns "339"
        Falls back to full id string if the pattern changes.
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
        # explicit new format first
        date_elem = element.find(class_="message-date")
        if date_elem:
            return date_elem.get_text(" ", strip=True)

        time_patterns = [
            ("time", {}),
            ("span", {"class": re.compile(r"time|date|timestamp", re.I)}),
            ("div", {"class": re.compile(r"time|date|timestamp", re.I)}),
        ]

        for tag, attrs in time_patterns:
            time_elem = element.find(tag, attrs)
            if time_elem:
                time_text = time_elem.get_text(" ", strip=True)
                if time_text:
                    return time_text

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

        sender_patterns = [
            ("span", {"class": re.compile(r"sender|from|author|name", re.I)}),
            ("div", {"class": re.compile(r"sender|from|author|name", re.I)}),
            ("strong", {}),
            ("b", {}),
        ]

        for tag, attrs in sender_patterns:
            sender_elem = element.find(tag, attrs)
            if sender_elem:
                sender = sender_elem.get_text(" ", strip=True)
                if sender and len(sender) < 200:
                    return sender

        return "Unknown"

    def _extract_message_text(self, element) -> str:
        """
        Extract full visible message text.
        The new format uses message-text and may include URLs inline.
        """
        text_elem = element.find(class_="message-text")
        if text_elem:
            return text_elem.get_text(" ", strip=True)

        message_patterns = [
            ("div", {"class": re.compile(r"message-content|msg-content|content|body|text", re.I)}),
            ("p", {"class": re.compile(r"message|msg|text", re.I)}),
            ("span", {"class": re.compile(r"message|msg|text", re.I)}),
        ]

        for tag, attrs in message_patterns:
            msg_elem = element.find(tag, attrs)
            if msg_elem:
                text = msg_elem.get_text(" ", strip=True)
                if text:
                    return text

        text = element.get_text(" ", strip=True)
        return text if text else ""

    def _extract_urls(self, element) -> List[Dict]:
        """Extract URLs from anchor tags and raw text."""
        urls = []
        seen_urls = set()

        for link in element.find_all("a", href=True):
            url = link.get("href", "").strip()
            display_text = link.get_text(" ", strip=True)

            if not url or url in seen_urls:
                continue

            if url.startswith(("javascript:", "mailto:", "#", "tel:")):
                continue

            urls.append({
                "url": url,
                "text": display_text if display_text else url,
                "type": self._classify_url(url)
            })
            seen_urls.add(url)

        text_content = element.get_text(" ", strip=True)
        url_pattern = r"http[s]?://(?:[a-zA-Z0-9$\-_.+!*'(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"

        for match in re.finditer(url_pattern, text_content):
            url = match.group(0)
            if url not in seen_urls:
                urls.append({
                    "url": url,
                    "text": url,
                    "type": self._classify_url(url)
                })
                seen_urls.add(url)

        return urls

    def _classify_url(self, url: str) -> str:
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc
            path = parsed.path

            if "sharepoint.com" in domain or "sharepoint." in domain:
                return "SharePoint"
            elif "teams.microsoft.com" in domain or "teams.live.com" in domain:
                return "Teams"
            elif "onedrive" in domain:
                return "OneDrive"
            elif any(d in domain for d in ["dropbox.com", "box.com", "drive.google.com"]):
                return "File Sharing"
            elif any(d in domain for d in ["zoom.us", "meet.google.com", "webex.com"]):
                return "Meeting"
            elif any(path.endswith(ext) for ext in [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx"]):
                return "Document"
            elif any(path.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".mp4", ".mp3"]):
                return "Media"
            return "Web"
        except Exception:
            return "Unknown"

    def _extract_attachments(self, element) -> List[Dict]:
        attachments = []

        attachment_patterns = [
            ("div", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("span", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("a", {"class": re.compile(r"attachment|file|document", re.I)}),
            ("li", {"class": re.compile(r"attachment|file|document", re.I)}),
        ]

        for tag, attrs in attachment_patterns:
            for att_elem in element.find_all(tag, attrs):
                attachment_info = self._parse_attachment_element(att_elem)
                if attachment_info and attachment_info not in attachments:
                    attachments.append(attachment_info)

        for link in element.find_all("a", href=True):
            href = link.get("href", "")
            if (
                "download" in href.lower()
                or any(ext in href.lower() for ext in [
                    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
                    ".zip", ".rar", ".txt", ".csv", ".jpg", ".png", ".gif", ".mp4"
                ])
            ):
                attachment_info = {
                    "filename": self._extract_filename_from_url(href),
                    "url": href,
                    "size": "Unknown",
                    "type": self._get_file_type(href)
                }
                if attachment_info not in attachments:
                    attachments.append(attachment_info)

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

        formatted = []
        for idx, url_info in enumerate(urls, 1):
            url = url_info["url"]
            text = url_info.get("text", url)
            url_type = url_info.get("type", "Web")
            formatted.append(f"[{idx}] {text} ({url_type}): {url}")
        return "\n".join(formatted)

    def _format_attachments_list(self, attachments: List[Dict]) -> str:
        if not attachments:
            return ""

        formatted = []
        for idx, att in enumerate(attachments, 1):
            filename = att.get("filename", "Unknown")
            file_type = att.get("type", "Unknown")
            size = att.get("size", "Unknown")
            url = att.get("url", "")

            line = f"[{idx}] {filename} ({file_type}, {size})"
            if url:
                line += f" - {url}"
            formatted.append(line)

        return "\n".join(formatted)

    def _generate_hash(self, message_id: str, timestamp: str, sender: str, message: str) -> str:
        """
        Include message_id in the hash when present.
        This improves dedupe and traceability.
        """
        content = f"{message_id}|{timestamp}|{sender}|{message}".encode("utf-8")
        return hashlib.md5(content).hexdigest()

    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Checking for duplicates...")
        initial_count = len(df)
        df = df.drop_duplicates(subset=["message_hash"], keep="first")
        final_count = len(df)

        duplicates = initial_count - final_count
        self.stats["duplicates_removed"] = duplicates

        if duplicates > 0:
            self.logger.info(f"Removed {duplicates} duplicate messages")
        else:
            self.logger.info("No duplicates found")

        return df.reset_index(drop=True)

    def save_to_excel(self, df: pd.DataFrame, output_file: str = None) -> Path:
        """Save extracted messages to Excel."""
        if output_file:
            output_path = Path(output_file)
        else:
            output_path = self.output_dir / f"{self.html_file.stem}_parsed.xlsx"

        preferred_columns = [
            "msg_index",
            "message_id",
            "timestamp",
            "sender",
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
                {"Metric": "URLs Extracted", "Value": self.stats["urls_extracted"]},
                {"Metric": "Attachments Found", "Value": self.stats["attachments_found"]},
                {"Metric": "Messages With URLs", "Value": self.stats["messages_with_urls"]},
                {"Metric": "Messages With Attachments", "Value": self.stats["messages_with_attachments"]},
            ])
            summary.to_excel(writer, index=False, sheet_name="Summary")

        self.logger.info(f"Saved Excel output to: {output_path}")
        return output_path


def convert_teams_chat(html_file: str, output_dir: str = None) -> tuple[str, str]:
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
    excel_path = converter.save_to_excel(df)

    return str(excel_path), str(converter.log_file)
