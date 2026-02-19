"""
Purview Teams Chat Converter
Converts HTML Teams chat exports to Excel with deduplication, timestamp drift detection,
URL extraction, and attachment tracking.
"""

import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime, timedelta
import logging
import re
from typing import List, Dict, Tuple, Set
import hashlib
from urllib.parse import urlparse, unquote


class TeamsChartConverter:
    """Main converter class for Teams chat HTML to Excel."""
    
    def __init__(self, html_file: str, output_dir: str = None):
        """
        Initialize converter.
        
        Args:
            html_file: Path to HTML export file
            output_dir: Directory for output files (defaults to same as input)
        """
        self.html_file = Path(html_file)
        self.output_dir = Path(output_dir) if output_dir else self.html_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self.log_file = self.output_dir / f"{self.html_file.stem}_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self._setup_logging()
        
        # Stats tracking
        self.stats = {
            'total_messages': 0,
            'duplicates_removed': 0,
            'timestamp_drifts': 0,
            'errors': 0,
            'processing_time': 0,
            'urls_extracted': 0,
            'attachments_found': 0,
            'messages_with_urls': 0,
            'messages_with_attachments': 0
        }
        
    def _setup_logging(self):
        """Configure logging to file and console."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def parse_html(self) -> pd.DataFrame:
        """
        Parse HTML file and extract chat messages.
        
        Returns:
            DataFrame with chat messages
        """
        self.logger.info(f"Parsing HTML file: {self.html_file}")
        
        try:
            with open(self.html_file, 'r', encoding='utf-8') as f:
                html_content = f.read()
        except Exception as e:
            self.logger.error(f"Error reading HTML file: {e}")
            raise
        
        soup = BeautifulSoup(html_content, 'html.parser')
        messages = []
        
        # Common patterns for Purview Teams exports
        message_elements = self._find_message_elements(soup)
        
        for idx, element in enumerate(message_elements):
            try:
                message_data = self._extract_message_data(element, idx)
                if message_data:
                    messages.append(message_data)
            except Exception as e:
                self.logger.warning(f"Error parsing message {idx}: {e}")
                self.stats['errors'] += 1
        
        self.stats['total_messages'] = len(messages)
        self.logger.info(f"Extracted {len(messages)} messages from HTML")
        
        return pd.DataFrame(messages)
    
    def _find_message_elements(self, soup):
        """
        Find message elements in HTML. Tries multiple common patterns.
        
        Args:
            soup: BeautifulSoup object
            
        Returns:
            List of message elements
        """
        # Try different selectors for Purview exports
        selectors = [
            ('div', {'class': 'message'}),
            ('div', {'class': 'chat-message'}),
            ('div', {'class': 'msg'}),
            ('tr', {'class': 'message-row'}),
            ('div', {'data-type': 'message'}),
            ('div', {'class': re.compile('MessageCard|message-card', re.I)}),
        ]
        
        for tag, attrs in selectors:
            elements = soup.find_all(tag, attrs)
            if elements:
                self.logger.info(f"Found {len(elements)} messages using selector: {tag} {attrs}")
                return elements
        
        # Fallback: try generic patterns
        elements = soup.find_all('div', class_=re.compile('message|msg|chat', re.I))
        if elements:
            self.logger.info(f"Found {len(elements)} messages using fallback pattern")
            return elements
        
        self.logger.warning("No messages found with standard selectors, trying all divs")
        return soup.find_all('div')
    
    def _extract_message_data(self, element, index: int) -> Dict:
        """
        Extract message data from HTML element including URLs and attachments.
        
        Args:
            element: BeautifulSoup element
            index: Message index
            
        Returns:
            Dictionary with message data
        """
        # Extract basic fields
        timestamp = self._extract_timestamp(element)
        sender = self._extract_sender(element)
        recipient = self._extract_recipient(element)
        message = self._extract_message_text(element)
        
        # Extract URLs and attachments
        urls = self._extract_urls(element)
        attachments = self._extract_attachments(element)
        
        # Update statistics
        if urls:
            self.stats['urls_extracted'] += len(urls)
            self.stats['messages_with_urls'] += 1
        
        if attachments:
            self.stats['attachments_found'] += len(attachments)
            self.stats['messages_with_attachments'] += 1
        
        # Only include if we have at least timestamp and message
        if message:
            return {
                'index': index,
                'timestamp': timestamp,
                'sender': sender,
                'recipient': recipient,
                'message': message,
                'urls': self._format_urls_list(urls),
                'url_count': len(urls),
                'attachments': self._format_attachments_list(attachments),
                'attachment_count': len(attachments),
                'has_urls': len(urls) > 0,
                'has_attachments': len(attachments) > 0,
                'message_hash': self._generate_hash(timestamp, sender, message)
            }
        
        return None
    
    def _extract_urls(self, element) -> List[Dict]:
        """
        Extract URLs from message element.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            List of URL dictionaries with url and display text
        """
        urls = []
        seen_urls = set()
        
        # Method 1: Extract from anchor tags
        for link in element.find_all('a', href=True):
            url = link.get('href', '').strip()
            display_text = link.get_text(strip=True)
            
            if url and url not in seen_urls:
                # Skip common non-URLs
                if url.startswith(('javascript:', 'mailto:', '#', 'tel:')):
                    continue
                
                urls.append({
                    'url': url,
                    'text': display_text if display_text else url,
                    'type': self._classify_url(url)
                })
                seen_urls.add(url)
        
        # Method 2: Extract URLs from text using regex
        text_content = element.get_text()
        url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
        
        for match in re.finditer(url_pattern, text_content):
            url = match.group(0)
            if url not in seen_urls:
                urls.append({
                    'url': url,
                    'text': url,
                    'type': self._classify_url(url)
                })
                seen_urls.add(url)
        
        return urls
    
    def _classify_url(self, url: str) -> str:
        """
        Classify URL type.
        
        Args:
            url: URL string
            
        Returns:
            URL type classification
        """
        try:
            parsed = urlparse(url.lower())
            domain = parsed.netloc
            path = parsed.path
            
            # Teams/SharePoint URLs
            if 'sharepoint.com' in domain or 'sharepoint.' in domain:
                return 'SharePoint'
            elif 'teams.microsoft.com' in domain or 'teams.live.com' in domain:
                return 'Teams'
            elif 'onedrive' in domain:
                return 'OneDrive'
            
            # Common file sharing
            elif any(d in domain for d in ['dropbox.com', 'box.com', 'drive.google.com']):
                return 'File Sharing'
            
            # Meeting links
            elif any(d in domain for d in ['zoom.us', 'meet.google.com', 'webex.com']):
                return 'Meeting'
            
            # Document extensions
            elif any(path.endswith(ext) for ext in ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx']):
                return 'Document'
            
            # Media
            elif any(path.endswith(ext) for ext in ['.jpg', '.jpeg', '.png', '.gif', '.mp4', '.mp3']):
                return 'Media'
            
            else:
                return 'Web'
                
        except:
            return 'Unknown'
    
    def _extract_attachments(self, element) -> List[Dict]:
        """
        Extract attachment information from message element.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            List of attachment dictionaries
        """
        attachments = []
        
        # Method 1: Look for attachment-specific elements
        attachment_patterns = [
            ('div', {'class': re.compile('attachment|file|document', re.I)}),
            ('span', {'class': re.compile('attachment|file|document', re.I)}),
            ('a', {'class': re.compile('attachment|file|document', re.I)}),
            ('li', {'class': re.compile('attachment|file|document', re.I)}),
        ]
        
        for tag, attrs in attachment_patterns:
            for att_elem in element.find_all(tag, attrs):
                attachment_info = self._parse_attachment_element(att_elem)
                if attachment_info:
                    attachments.append(attachment_info)
        
        # Method 2: Look for file icons or indicators
        file_icons = element.find_all(['img', 'i', 'span'], class_=re.compile('icon|file-icon', re.I))
        for icon in file_icons:
            parent = icon.parent
            if parent:
                attachment_info = self._parse_attachment_element(parent)
                if attachment_info and attachment_info not in attachments:
                    attachments.append(attachment_info)
        
        # Method 3: Look for download links
        for link in element.find_all('a', href=True):
            href = link.get('href', '')
            if 'download' in href.lower() or any(ext in href.lower() for ext in [
                '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                '.zip', '.rar', '.txt', '.csv', '.jpg', '.png', '.gif', '.mp4'
            ]):
                attachment_info = {
                    'filename': self._extract_filename_from_url(href),
                    'url': href,
                    'size': 'Unknown',
                    'type': self._get_file_type(href)
                }
                if attachment_info not in attachments:
                    attachments.append(attachment_info)
        
        return attachments
    
    def _parse_attachment_element(self, element) -> Dict:
        """
        Parse attachment information from element.
        
        Args:
            element: BeautifulSoup element
            
        Returns:
            Attachment dictionary or None
        """
        try:
            # Try to find filename
            filename = None
            
            # Look for title or data attributes
            for attr in ['title', 'data-filename', 'data-file', 'aria-label']:
                if element.get(attr):
                    filename = element.get(attr)
                    break
            
            # Try text content
            if not filename:
                text = element.get_text(strip=True)
                if text and len(text) > 0 and len(text) < 200:
                    filename = text
            
            # Try to find URL
            url = None
            if element.name == 'a' and element.get('href'):
                url = element.get('href')
            else:
                link = element.find('a', href=True)
                if link:
                    url = link.get('href')
            
            # Try to find file size
            size = 'Unknown'
            size_elem = element.find(['span', 'div'], class_=re.compile('size|filesize', re.I))
            if size_elem:
                size = size_elem.get_text(strip=True)
            
            if filename or url:
                return {
                    'filename': filename or 'Unknown',
                    'url': url or '',
                    'size': size,
                    'type': self._get_file_type(filename or url or '')
                }
        except:
            pass
        
        return None
    
    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL."""
        try:
            path = urlparse(url).path
            filename = unquote(path.split('/')[-1])
            if filename and '.' in filename:
                return filename
        except:
            pass
        return 'Unknown'
    
    def _get_file_type(self, filename: str) -> str:
        """Get file type from filename or URL."""
        filename_lower = filename.lower()
        
        extensions = {
            'Document': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt'],
            'Spreadsheet': ['.xls', '.xlsx', '.csv', '.ods'],
            'Presentation': ['.ppt', '.pptx', '.odp'],
            'Image': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.webp'],
            'Video': ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv'],
            'Audio': ['.mp3', '.wav', '.ogg', '.m4a', '.flac'],
            'Archive': ['.zip', '.rar', '.7z', '.tar', '.gz'],
            'Code': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.h'],
        }
        
        for file_type, exts in extensions.items():
            if any(filename_lower.endswith(ext) for ext in exts):
                return file_type
        
        return 'Other'
    
    def _format_urls_list(self, urls: List[Dict]) -> str:
        """Format URLs list for Excel cell."""
        if not urls:
            return ''
        
        formatted = []
        for idx, url_info in enumerate(urls, 1):
            url = url_info['url']
            text = url_info.get('text', url)
            url_type = url_info.get('type', 'Web')
            
            # Format: [1] Text (Type): URL
            formatted.append(f"[{idx}] {text} ({url_type}): {url}")
        
        return '\n'.join(formatted)
    
    def _format_attachments_list(self, attachments: List[Dict]) -> str:
        """Format attachments list for Excel cell."""
        if not attachments:
            return ''
        
        formatted = []
        for idx, att in enumerate(attachments, 1):
            filename = att.get('filename', 'Unknown')
            file_type = att.get('type', 'Unknown')
            size = att.get('size', 'Unknown')
            url = att.get('url', '')
            
            # Format: [1] filename.ext (Type, Size) - URL
            line = f"[{idx}] {filename} ({file_type}, {size})"
            if url:
                line += f" - {url}"
            formatted.append(line)
        
        return '\n'.join(formatted)
    
    def _extract_timestamp(self, element):
        """Extract timestamp from element."""
        # Try common timestamp patterns
        time_patterns = [
            ('time', {}),
            ('span', {'class': re.compile('time|date|timestamp', re.I)}),
            ('div', {'class': re.compile('time|date|timestamp', re.I)}),
        ]
        
        for tag, attrs in time_patterns:
            time_elem = element.find(tag, attrs)
            if time_elem:
                time_text = time_elem.get_text(strip=True)
                parsed_time = self._parse_timestamp(time_text)
                if parsed_time:
                    return parsed_time
        
        # Try datetime attribute
        for attr in ['datetime', 'data-timestamp', 'data-time']:
            if element.get(attr):
                parsed_time = self._parse_timestamp(element.get(attr))
                if parsed_time:
                    return parsed_time
        
        return None
    
    def _parse_timestamp(self, time_str: str):
        """Parse timestamp string to datetime."""
        if not time_str:
            return None
        
        # Common timestamp formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S.%fZ',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %I:%M:%S %p',
            '%d/%m/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M',
            '%m/%d/%Y %H:%M',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(time_str.strip(), fmt)
            except ValueError:
                continue
        
        # Try pandas parsing as last resort
        try:
            return pd.to_datetime(time_str)
        except:
            return None
    
    def _extract_sender(self, element):
        """Extract sender from element."""
        sender_patterns = [
            ('span', {'class': re.compile('sender|from|author|name', re.I)}),
            ('div', {'class': re.compile('sender|from|author|name', re.I)}),
            ('strong', {}),
            ('b', {}),
        ]
        
        for tag, attrs in sender_patterns:
            sender_elem = element.find(tag, attrs)
            if sender_elem:
                sender = sender_elem.get_text(strip=True)
                if sender and len(sender) > 0 and len(sender) < 100:
                    return sender
        
        return 'Unknown'
    
    def _extract_recipient(self, element):
        """Extract recipient from element."""
        recipient_patterns = [
            ('span', {'class': re.compile('recipient|to', re.I)}),
            ('div', {'class': re.compile('recipient|to', re.I)}),
        ]
        
        for tag, attrs in recipient_patterns:
            recipient_elem = element.find(tag, attrs)
            if recipient_elem:
                return recipient_elem.get_text(strip=True)
        
        return 'Unknown'
    
    def _extract_message_text(self, element):
        """Extract message text from element."""
        # Try specific message content patterns
        message_patterns = [
            ('div', {'class': re.compile('message-content|msg-content|content|body|text', re.I)}),
            ('p', {'class': re.compile('message|msg|text', re.I)}),
            ('span', {'class': re.compile('message|msg|text', re.I)}),
        ]
        
        for tag, attrs in message_patterns:
            msg_elem = element.find(tag, attrs)
            if msg_elem:
                text = msg_elem.get_text(strip=True)
                if text and len(text) > 0:
                    return text
        
        # Fallback: get all text from element
        text = element.get_text(strip=True)
        return text if text else None
    
    def _generate_hash(self, timestamp, sender: str, message: str) -> str:
        """Generate hash for duplicate detection."""
        content = f"{timestamp}{sender}{message}".encode('utf-8')
        return hashlib.md5(content).hexdigest()
    
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate messages based on hash.
        
        Args:
            df: DataFrame with messages
            
        Returns:
            DataFrame with duplicates removed
        """
        self.logger.info("Checking for duplicates...")
        
        initial_count = len(df)
        df = df.drop_duplicates(subset=['message_hash'], keep='first')
        final_count = len(df)
        
        duplicates = initial_count - final_count
        self.stats['duplicates_removed'] = duplicates
        
        if duplicates > 0:
            self.logger.info(f"Removed {duplicates} duplicate messages")
        else:
            self.logger.info("No duplicates found")
        
        return df.reset_index(drop=True)
    
    def check_timestamp_drift(self, df: pd.DataFrame, threshold_seconds: int = 300) -> pd.DataFrame:
        """
        Check for timestamp drift (messages out of order).
        
        Args:
            df: DataFrame with messages
            threshold_seconds: Maximum acceptable drift in seconds
            
        Returns:
            DataFrame with drift flag added
        """
        self.logger.info("Checking for timestamp drift...")
        
        if 'timestamp' not in df.columns or df['timestamp'].isna().all():
            self.logger.warning("No valid timestamps to check for drift")
            df['has_drift'] = False
            return df
        
        # Sort by index to check original order
        df = df.sort_values('index').reset_index(drop=True)
        
        # Calculate time differences
        df['time_diff'] = df['timestamp'].diff()
        
        # Flag messages with negative drift beyond threshold
        df['has_drift'] = (df['time_diff'] < timedelta(seconds=-threshold_seconds))
        
        drift_count = df['has_drift'].sum()
        self.stats['timestamp_drifts'] = drift_count
        
        if drift_count > 0:
            self.logger.warning(f"Found {drift_count} messages with timestamp drift > {threshold_seconds}s")
            
            # Log specific drift instances
            drift_messages = df[df['has_drift']][['index', 'timestamp', 'time_diff', 'sender', 'message']]
            for _, row in drift_messages.head(10).iterrows():
                self.logger.warning(f"  Drift at index {row['index']}: {row['time_diff']} - {row['sender']}")
        else:
            self.logger.info("No significant timestamp drift detected")
        
        # Sort by timestamp for output
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        return df
    
    def save_to_excel(self, df: pd.DataFrame) -> str:
        """
        Save DataFrame to Excel with formatting.
        
        Args:
            df: DataFrame to save
            
        Returns:
            Path to output file
        """
        output_file = self.output_dir / f"{self.html_file.stem}_converted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        self.logger.info(f"Saving to Excel: {output_file}")
        
        # Select and order columns for output
        output_columns = [
            'timestamp', 'sender', 'recipient', 'message',
            'url_count', 'urls', 'attachment_count', 'attachments', 'has_drift'
        ]
        
        # Only include columns that exist
        output_columns = [col for col in output_columns if col in df.columns]
        export_df = df[output_columns].copy()
        
        # Format timestamp
        if 'timestamp' in export_df.columns:
            export_df['timestamp'] = export_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Write to Excel with formatting
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            export_df.to_excel(writer, index=False, sheet_name='Teams Chats')
            
            # Format worksheet
            worksheet = writer.sheets['Teams Chats']
            
            # Set column widths
            column_widths = {
                'A': 20,  # timestamp
                'B': 25,  # sender
                'C': 25,  # recipient
                'D': 50,  # message
                'E': 10,  # url_count
                'F': 60,  # urls
                'G': 12,  # attachment_count
                'H': 60,  # attachments
                'I': 12,  # has_drift
            }
            
            for col, width in column_widths.items():
                worksheet.column_dimensions[col].width = width
            
            # Enable text wrapping for long columns
            from openpyxl.styles import Alignment
            for row in worksheet.iter_rows(min_row=2, max_row=worksheet.max_row):
                for cell in row:
                    cell.alignment = Alignment(wrap_text=True, vertical='top')
            
            # Freeze header row
            worksheet.freeze_panes = 'A2'
            
            # Bold header
            from openpyxl.styles import Font
            for cell in worksheet[1]:
                cell.font = Font(bold=True)
        
        self.logger.info(f"Excel file saved: {output_file}")
        return str(output_file)
    
    def generate_summary_report(self, df: pd.DataFrame):
        """
        Generate and log summary statistics.
        
        Args:
            df: Processed DataFrame
        """
        self.logger.info("\n" + "=" * 60)
        self.logger.info("PROCESSING SUMMARY REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Input file: {self.html_file}")
        self.logger.info(f"Total messages extracted: {self.stats['total_messages']:,}")
        self.logger.info(f"Duplicates removed: {self.stats['duplicates_removed']:,}")
        self.logger.info(f"Timestamp drifts detected: {self.stats['timestamp_drifts']:,}")
        self.logger.info(f"Parsing errors: {self.stats['errors']:,}")
        self.logger.info(f"Final message count: {len(df):,}")
        
        # URL statistics
        self.logger.info(f"\n--- URL Statistics ---")
        self.logger.info(f"Total URLs extracted: {self.stats['urls_extracted']:,}")
        self.logger.info(f"Messages with URLs: {self.stats['messages_with_urls']:,}")
        if self.stats['messages_with_urls'] > 0:
            avg_urls = self.stats['urls_extracted'] / self.stats['messages_with_urls']
            self.logger.info(f"Average URLs per message (with URLs): {avg_urls:.2f}")
        
        # Attachment statistics
        self.logger.info(f"\n--- Attachment Statistics ---")
        self.logger.info(f"Total attachments found: {self.stats['attachments_found']:,}")
        self.logger.info(f"Messages with attachments: {self.stats['messages_with_attachments']:,}")
        if self.stats['messages_with_attachments'] > 0:
            avg_attachments = self.stats['attachments_found'] / self.stats['messages_with_attachments']
            self.logger.info(f"Average attachments per message (with attachments): {avg_attachments:.2f}")
        
        if not df.empty and 'timestamp' in df.columns:
            valid_timestamps = df['timestamp'].dropna()
            if not valid_timestamps.empty:
                self.logger.info(f"\n--- Date Range ---")
                self.logger.info(f"From: {valid_timestamps.min()}")
                self.logger.info(f"To: {valid_timestamps.max()}")
        
        if not df.empty and 'sender' in df.columns:
            self.logger.info(f"\n--- Sender Statistics ---")
            self.logger.info(f"Unique senders: {df['sender'].nunique()}")
            self.logger.info("\nTop 5 senders:")
            for sender, count in df['sender'].value_counts().head().items():
                self.logger.info(f"  {sender}: {count:,} messages")
        
        self.logger.info(f"\n--- Processing ---")
        self.logger.info(f"Processing time: {self.stats['processing_time']:.2f} seconds")
        self.logger.info(f"Log file: {self.log_file}")
        self.logger.info("=" * 60)
    
    def convert(self) -> Tuple[str, str]:
        """
        Main conversion process.
        
        Returns:
            Tuple of (excel_file_path, log_file_path)
        """
        start_time = datetime.now()
        
        try:
            self.logger.info("Starting conversion process...")
            
            # Parse HTML
            df = self.parse_html()
            
            if df.empty:
                self.logger.error("No messages extracted from HTML file")
                raise ValueError("No messages found in HTML file")
            
            # Remove duplicates
            df = self.remove_duplicates(df)
            
            # Check timestamp drift
            df = self.check_timestamp_drift(df)
            
            # Save to Excel
            excel_file = self.save_to_excel(df)
            
            # Calculate processing time
            self.stats['processing_time'] = (datetime.now() - start_time).total_seconds()
            
            # Generate summary
            self.generate_summary_report(df)
            
            self.logger.info("Conversion completed successfully!")
            
            return excel_file, str(self.log_file)
            
        except Exception as e:
            self.logger.error(f"Conversion failed: {e}", exc_info=True)
            raise


def convert_teams_chat(html_file: str, output_dir: str = None) -> Tuple[str, str]:
    """
    Convenience function for converting Teams chat HTML to Excel.
    
    Args:
        html_file: Path to HTML file
        output_dir: Output directory (optional)
        
    Returns:
        Tuple of (excel_file, log_file)
    """
    converter = TeamsChartConverter(html_file, output_dir)
    return converter.convert()


if __name__ == "__main__":
    # Example usage
    import sys
    
    if len(sys.argv) > 1:
        html_file = sys.argv[1]
        excel_file, log_file = convert_teams_chat(html_file)
        print(f"\nConversion complete!")
        print(f"Excel: {excel_file}")
        print(f"Log: {log_file}")
    else:
        print("Usage: python teams_chat_converter.py <html_file>")
