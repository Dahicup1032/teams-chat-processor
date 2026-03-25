import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
import logging
from typing import Tuple
import hashlib


class TeamsChatConverter:

    def __init__(self, html_file: str, output_dir: str = None):
        self.html_file = Path(html_file)
        self.output_dir = Path(output_dir) if output_dir else self.html_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.log_file = self.output_dir / f"{self.html_file.stem}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self._setup_logging()

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
        messages = soup.find_all("label", class_="unknown-direction-message-wrapper")

        self.logger.info(f"Found {len(messages)} message elements")

        rows = []
        for i, m in enumerate(messages, 1):
            sender = m.find(class_="message-sender")
            text = m.find(class_="message-text")
            time = m.find(class_="message-date")

            row = {
                "index": i,
                "timestamp": time.get_text(strip=True) if time else "",
                "sender": sender.get_text(strip=True) if sender else "Unknown",
                "recipient": participants,
                "message": text.get_text(" ", strip=True) if text else "",
            }

            row["message_hash"] = hashlib.md5(
                f"{row['index']}{row['sender']}{row['message']}".encode()
            ).hexdigest()

            rows.append(row)

        self.logger.info(f"Extracted {len(rows)} messages")
        return pd.DataFrame(rows)

    # =========================
    # PARTICIPANTS FIX
    # =========================
    def _extract_participants(self, soup) -> str:
        for row in soup.find_all("tr"):
            label = row.find("td", class_="chat-label")
            if not label:
                continue

            if label.get_text(strip=True).lower() != "display names":
                continue

            data = row.find("td", class_="chat-data")
            if not data:
                return ""

            nested = data.find_all("div", class_="chat-data")

            if nested:
                names = [n.get_text(strip=True) for n in nested if n.get_text(strip=True)]
                return "; ".join(names)

            return data.get_text(strip=True)

        return ""

    # =========================
    # SAVE
    # =========================
    def save_to_excel(self, df):

        output_file = self.output_dir / f"{self.html_file.stem}_parsed.xlsx"

        # SAFE timestamp conversion (no crash)
        if "timestamp" in df.columns:
            ts = pd.to_datetime(df["timestamp"], errors="coerce")
            df["timestamp"] = ts.dt.strftime("%Y-%m-%d %H:%M:%S")
            df["timestamp"] = df["timestamp"].fillna("")

        df.to_excel(output_file, index=False)

        self.logger.info(f"Saved: {output_file}")
        return output_file


# =========================
# ENTRY POINT
# =========================
def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:

    converter = TeamsChatConverter(html_file, output_dir)

    df = converter.parse_html()

    excel_file = converter.save_to_excel(df)

    return str(excel_file), str(converter.log_file)
