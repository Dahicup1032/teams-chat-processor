import pandas as pd
import re
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(filename='teams_chat_converter.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class TeamsChatConverter:
    def __init__(self, input_directory, output_file):
        self.input_directory = input_directory
        self.output_file = output_file
        self.messages = []
        self.processed_urls = set()

    def extract_messages(self):
        logging.info('Starting message extraction.')
        for filename in os.listdir(self.input_directory):
            if filename.endswith('.html'):
                file_path = os.path.join(self.input_directory, filename)
                with open(file_path, 'r', encoding='utf-8') as file:
                    content = file.read()
                    self._parse_html(content)
        self.messages = list(set(self.messages))  # Remove duplicates
        logging.info(f'Extracted {len(self.messages)} unique messages.')

    def _parse_html(self, content):
        messages = re.findall(r'<div class="message">(.*?)</div>', content, re.DOTALL)
        for message in messages:
            self.messages.append(self._clean_message(message))

    def _clean_message(self, message):
        # Remove URLs and clean text
        url_matches = re.findall(r'(https?://[\w\.-]+)\S*', message)
        for url in url_matches:
            self.processed_urls.add(url)
        text = re.sub(r'https?://[\w\.-]+\S*', '', message)
        return text.strip()

    def check_timestamp_drift(self, timestamps):
        if not timestamps:
            return
        sorted_timestamps = sorted(timestamps)
        for i in range(1, len(sorted_timestamps)):
            if (sorted_timestamps[i] - sorted_timestamps[i-1]).total_seconds() > 60:
                logging.warning('Timestamp drift detected between messages.')

    def export_to_excel(self):
        df = pd.DataFrame({'Messages': self.messages})
        df['Processed URLs'] = pd.Series(list(self.processed_urls))
        df.to_excel(self.output_file, index=False)
        logging.info(f'Exported data to {self.output_file}.')

# Example usage
if __name__ == '__main__':
    converter = TeamsChatConverter(input_directory='input_html_files', output_file='output.xlsx')
    converter.extract_messages()
    # Check for timestamp drift with fake timestamps for demonstration
    converter.check_timestamp_drift([datetime.now() for _ in range(10)])  # Example timestamps
    converter.export_to_excel()
