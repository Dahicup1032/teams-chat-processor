import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
import re
from typing import List, Dict, Optional, Tuple
import hashlib


class TeamsChatConverter:

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

    # =========================
    # MAIN PARSER
    # =========================
    def parse_html(self) -> pd.DataFrame:

        with open(self.html_file, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f.read(), "html.parser")

        participants = self._extract_participants(soup)
        messages = self._find_message_elements(soup)

        rows = []
        for i, m in enumerate(messages, 1):
            row = self._extract_message(m, i, participants)
            if row:
                rows.append(row)

        self.logger.info(f"Extracted {len(rows)} messages")
        return pd.DataFrame(rows)

    # =========================
    # PARTICIPANTS (RECIPIENT FIX)
    # =========================
    def _extract_participants(self, soup) -> str:

        names = []
        for td in soup.find_all("td", class_="chat-data"):
            text = td.get_text(strip=True)
            if text and not text.lower().startswith("user count"):
                names.append(text)

        return "; ".join(names)

    # =========================
    # MESSAGE FINDER (FIXED)
    # =========================
    def _find_message_elements(self, soup):

        msgs = soup.find_all("label", class_="unknown-direction-message-wrapper")

        self.logger.info(f"Found {len(msgs)} message elements")
        return msgs

    # =========================
    # MESSAGE EXTRACTION
    # =========================
    def _extract_message(self, element, idx, participants):

        sender = element.find("div", class_="message-sender")
        text = element.find("div", class_="message-text")
        time = element.find("div", class_="message-date")

        return {
            "index": idx,
            "timestamp": time.get_text(strip=True) if time else "",
            "sender": sender.get_text(strip=True) if sender else "Unknown",
            "recipient": participants,
            "message": text.get_text(" ", strip=True) if text else "",
            "message_hash": self._hash(idx, sender, text),
        }

    def _hash(self, idx, sender, text):
        raw = f"{idx}{sender}{text}".encode()
        return hashlib.md5(raw).hexdigest()

    # =========================
    # CLEANUP
    # =========================
    def remove_duplicates(self, df):
        if "message_hash" not in df:
            return df
        return df.drop_duplicates("message_hash").reset_index(drop=True)

    def check_timestamp_drift(self, df):
        return df

    # =========================
    # SAVE (FIXED TIMESTAMP BUG)
    # =========================
    def save_to_excel(self, df):

        out = self.output_dir / f"{self.html_file.stem}_parsed.xlsx"

        # FIX: safe timestamp conversion
        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], errors="coerce")
            df["timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
            df["timestamp"] = df["timestamp"].fillna("")

        df.to_excel(out, index=False)

        self.logger.info(f"Saved: {out}")
        return out


# =========================
# ENTRY FUNCTION
# =========================
def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:

    c = TeamsChatConverter(html_file, output_dir)

    df = c.parse_html()

    df = c.remove_duplicates(df)
    df = c.check_timestamp_drift(df)

    excel = c.save_to_excel(df)

    return str(excel), str(c.log_file)
