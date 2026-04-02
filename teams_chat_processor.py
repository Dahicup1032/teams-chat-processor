import os
import csv
import re
import json
import pandas as pd
import hashlib
from bs4 import BeautifulSoup
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import logging
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading

# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    def __init__(self):
        self.html_chat_folder = None
        self.output_folder = None
        self.output_csv = 'teams_chats_extracted.csv'
        self.output_excel = 'teams_chats_extracted.xlsx'
        self.log_file = 'processing_log.txt'
        self.summary_json = 'processing_summary.json'

config = Config()

# ============================================================================
# LOGGING CLASS
# ============================================================================

class ProcessingLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        self.logs = []
        self.start_time = datetime.now()
        self.gui_callback = None
        
    def add(self, level, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level.upper()}] {message}"
        self.logs.append(log_entry)
        print(log_entry)
        
        # Send to GUI if callback exists
        if self.gui_callback:
            self.gui_callback(log_entry)
    
    def write_to_file(self):
        with open(self.log_file, 'w', encoding='utf-8') as f:
            for log in self.logs:
                f.write(log + '\n')

# ============================================================================
# MESSAGE PROCESSING & DEDUPLICATION
# ============================================================================

def generate_message_hash(sender, timestamp, text, links='', attachments=''):
    """Generate a hash for duplicate detection"""
    combined = f"{sender}||{timestamp}||{text}||{links}||{attachments}".strip()
    return hashlib.md5(combined.encode()).hexdigest()

def extract_messages_from_html(html_content, source_file=''):
    """Parse Teams chat HTML and extract messages with recipient tracking"""
    soup = BeautifulSoup(html_content, 'lxml')
    messages = []

    def unique_preserve_order(values):
        seen = set()
        ordered = []
        for value in values:
            value = (value or '').strip()
            if value and value not in seen:
                seen.add(value)
                ordered.append(value)
        return ordered

    for msg_element in soup.find_all(['div', 'li'], class_=['message', 'chat-message', 'ms-Message']):
        sender = None
        timestamp = None
        text = ''
        recipients = []
        link_targets = []
        attachment_items = []

        sender_elem = msg_element.find('span', class_=['sender', 'author', 'ms-Persona-initials'])
        if sender_elem:
            sender = sender_elem.get_text(strip=True)
        else:
            sender = "Unknown"

        time_elem = msg_element.find('span', class_=['timestamp', 'time', 'ms-timestamp'])
        if time_elem:
            timestamp = time_elem.get_text(strip=True)

        text_elem = msg_element.find('span', class_=['text', 'content', 'message-text'])
        if text_elem:
            text = text_elem.get_text('\n', strip=True)
        else:
            text = msg_element.get_text(' ', strip=True)

        recipient_elems = msg_element.find_all('span', class_=['recipient', 'to', 'ms-recipient'])
        for recipient_elem in recipient_elems:
            recipients.append(recipient_elem.get_text(strip=True))

        for anchor in msg_element.find_all('a'):
            href = (anchor.get('href') or '').strip()
            label = anchor.get_text(' ', strip=True)
            classes = ' '.join(anchor.get('class', [])).lower()

            if href:
                link_targets.append(href)

            is_attachment_anchor = (
                'attachment' in classes or
                'file' in classes or
                'download' in classes or
                href.lower().endswith((
                    '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
                    '.csv', '.txt', '.zip', '.png', '.jpg', '.jpeg', '.gif'
                ))
            )

            if is_attachment_anchor:
                if label and href:
                    attachment_items.append(f"{label} ({href})")
                elif href:
                    attachment_items.append(href)
                elif label:
                    attachment_items.append(label)

        attachment_class_elems = msg_element.find_all(
            attrs={'class': re.compile(r'(attachment|file|download)', re.I)}
        )
        for elem in attachment_class_elems:
            elem_text = elem.get_text(' ', strip=True)
            if elem_text:
                attachment_items.append(elem_text)

        link_targets = unique_preserve_order(link_targets)
        attachment_items = unique_preserve_order(attachment_items)
        links_str = ' | '.join(link_targets) if link_targets else 'N/A'
        attachments_str = ' | '.join(attachment_items) if attachment_items else 'N/A'

        if text and sender:
            message_hash = generate_message_hash(sender, timestamp, text, links_str, attachments_str)
            messages.append({
                'Sender': sender,
                'Timestamp': timestamp,
                'Message': text,
                'Recipients': ','.join(recipients) if recipients else 'N/A',
                'Links': links_str,
                'Attachments': attachments_str,
                'Source_File': source_file,
                'Message_Hash': message_hash
            })

    return messages

def detect_timestamp_drift(df):
    """Detect timestamp inconsistencies/drift"""
    drift_issues = []

    if df.empty or 'Timestamp' not in df.columns:
        return drift_issues

    try:
        df['Timestamp_Parsed'] = pd.to_datetime(df['Timestamp'], errors='coerce')
        valid_timestamps = df[df['Timestamp_Parsed'].notna()].sort_values('Timestamp_Parsed')

        if len(valid_timestamps) < 2:
            return drift_issues

        for i in range(1, len(valid_timestamps)):
            prev_time = valid_timestamps.iloc[i-1]['Timestamp_Parsed']
            curr_time = valid_timestamps.iloc[i]['Timestamp_Parsed']

            if curr_time < prev_time:
                drift_issues.append({
                    'Type': 'Timestamp Reversal',
                    'Message_1': valid_timestamps.iloc[i-1]['Sender'],
                    'Time_1': prev_time,
                    'Message_2': valid_timestamps.iloc[i]['Sender'],
                    'Time_2': curr_time,
                    'Issue': f"Message went backwards from {prev_time} to {curr_time}"
                })

        valid_timestamps['Time_Diff'] = valid_timestamps['Timestamp_Parsed'].diff()
        large_gaps = valid_timestamps[valid_timestamps['Time_Diff'] > pd.Timedelta(hours=24)]

        for idx, row in large_gaps.iterrows():
            if pd.notna(row['Time_Diff']):
                drift_issues.append({
                    'Type': 'Large Time Gap',
                    'Sender': row['Sender'],
                    'Timestamp': row['Timestamp'],
                    'Gap_Hours': row['Time_Diff'].total_seconds() / 3600,
                    'Issue': f"Large gap detected: {row['Time_Diff']}"
                })

    except Exception as e:
        logger.add('warning', f"Could not parse timestamps for drift detection: {e}")

    return drift_issues
# ============================================================================
# PROCESSING FUNCTION
# ============================================================================

def process_chat_exports(logger):
    """Main processing function"""
    
    logger.add('info', '=' * 70)
    logger.add('info', 'STARTING MICROSOFT PURVIEW TEAMS CHAT PROCESSING')
    logger.add('info', f'Start Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.add('info', '=' * 70)

    all_messages = []
    file_count = 0
    error_count = 0
    message_count = 0

    logger.add('info', f'Scanning folder: {config.html_chat_folder}')

    if not os.path.exists(config.html_chat_folder):
        logger.add('error', f'Folder not found: {config.html_chat_folder}')
        return False
    
    html_files = [f for f in os.listdir(config.html_chat_folder) if f.endswith('.html')]
    logger.add('info', f'Found {len(html_files)} HTML files to process')
    
    for filename in html_files:
        filepath = os.path.join(config.html_chat_folder, filename)
        
        try:
            with open(filepath, encoding='utf-8') as f:
                html_data = f.read()
                msgs = extract_messages_from_html(html_data, source_file=filename)
                all_messages.extend(msgs)
                message_count += len(msgs)
                file_count += 1
                logger.add('info', f'✓ {filename} → {len(msgs)} messages extracted')
        
        except Exception as e:
            error_count += 1
            logger.add('error', f'✗ {filename} → Error: {str(e)}')

    logger.add('info', f'File scanning complete. {file_count} files processed.')

    # ========== DUPLICATE DETECTION ==========
    logger.add('info', '-' * 70)
    logger.add('info', 'DUPLICATE DETECTION PHASE')

    df = pd.DataFrame(all_messages)

    if not df.empty:
        initial_record_count = len(df)
        df['Is_Duplicate'] = df.duplicated(subset=['Message_Hash'], keep='first')
        duplicate_count = df['Is_Duplicate'].sum()
        
        logger.add('info', f'Initial records: {initial_record_count}')
        logger.add('info', f'Duplicates found: {duplicate_count}')
        
        if duplicate_count > 0:
            logger.add('warning', 'Duplicate messages detected')
        
        df_clean = df[~df['Is_Duplicate']].copy()
        final_record_count = len(df_clean)
        
        logger.add('info', f'Records after deduplication: {final_record_count}')
    else:
        logger.add('warning', 'No messages extracted from files')
        df_clean = df
        duplicate_count = 0

    # ========== TIMESTAMP DRIFT DETECTION ==========
    logger.add('info', '-' * 70)
    logger.add('info', 'TIMESTAMP DRIFT DETECTION PHASE')

    drift_issues = detect_timestamp_drift(df_clean)

    if drift_issues:
        logger.add('warning', f'Found {len(drift_issues)} timestamp anomalies')
    else:
        logger.add('info', '✓ No timestamp drifts or reversals detected')

    # ========== STATISTICS ==========
    logger.add('info', '-' * 70)
    logger.add('info', 'PROCESSING STATISTICS')

    summary = {
        'Processing_Timestamp': datetime.now().isoformat(),
        'Total_Files_Processed': file_count,
        'Total_Messages_Extracted': message_count,
        'Errors_Encountered': error_count,
        'Duplicate_Messages_Removed': duplicate_count if not df.empty else 0,
        'Final_Message_Count': len(df_clean),
        'Timestamp_Issues_Found': len(drift_issues),
        'Unique_Senders': df_clean['Sender'].nunique() if not df_clean.empty else 0,
        'Date_Range': {
            'Start': str(df_clean['Timestamp'].min()) if not df_clean.empty else 'N/A',
            'End': str(df_clean['Timestamp'].max()) if not df_clean.empty else 'N/A'
        }
    }

    logger.add('info', f"Files processed: {summary['Total_Files_Processed']}")
    logger.add('info', f"Final clean records: {summary['Final_Message_Count']}")
    logger.add('info', f"Unique senders: {summary['Unique_Senders']}")

    # ========== EXPORT DATA ==========
    logger.add('info', '-' * 70)
    logger.add('info', 'EXPORT PHASE')

    if not df_clean.empty:
        export_df = df_clean.drop(columns=['Message_Hash', 'Is_Duplicate'], errors='ignore')
        
        csv_path = os.path.join(config.output_folder, config.output_csv)
        excel_path = os.path.join(config.output_folder, config.output_excel)
        
        export_df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.add('info', f'✓ Exported to CSV: {csv_path}')
        
        export_df.to_excel(excel_path, index=False, engine='openpyxl')
        logger.add('info', f'✓ Exported to Excel: {excel_path}')
    else:
        logger.add('warning', 'No data to export')

    summary_path = os.path.join(config.output_folder, config.summary_json)
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    logger.add('info', f'✓ Summary exported to: {summary_path}')

    # ========== FINAL REPORT ==========
    logger.add('info', '=' * 70)
    logger.add('info', 'PROCESSING COMPLETE')
    logger.add('info', f'End Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    logger.add('info', '=' * 70)

    log_path = os.path.join(config.output_folder, config.log_file)
    logger.log_file = log_path
    logger.write_to_file()
    logger.add('info', f'✓ Processing log saved to: {log_path}')

    return True

# ============================================================================
# GUI APPLICATION
# ============================================================================

class TeamsChatProcessorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Teams Chat Processor")
        self.root.geometry("700x600")
        self.root.resizable(False, False)
        
        # Main frame
        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Title
        title = ttk.Label(main_frame, text="Microsoft Purview Teams Chat Processor", 
                         font=('Arial', 14, 'bold'))
        title.grid(row=0, column=0, columnspan=3, pady=10)
        
        # Input folder
        ttk.Label(main_frame, text="Select Export Folder:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.input_folder_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.input_folder_var, width=50).grid(row=1, column=1, padx=5)
        ttk.Button(main_frame, text="Browse", command=self.select_input_folder).grid(row=1, column=2, padx=5)
        
        # Output folder
        ttk.Label(main_frame, text="Select Output Folder:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.output_folder_var = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.output_folder_var, width=50).grid(row=2, column=1, padx=5)
        ttk.Button(main_frame, text="Browse", command=self.select_output_folder).grid(row=2, column=2, padx=5)
        
        # Process button
        ttk.Button(main_frame, text="Start Processing", command=self.start_processing).grid(row=3, column=0, columnspan=3, pady=20)
        
        # Log display
        ttk.Label(main_frame, text="Processing Log:").grid(row=4, column=0, columnspan=3, sticky=tk.W, pady=(10, 5))
        
        self.log_text = tk.Text(main_frame, height=20, width=80, state=tk.DISABLED)
        self.log_text.grid(row=5, column=0, columnspan=3, pady=5)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.grid(row=5, column=3, sticky=(tk.N, tk.S))
        self.log_text.config(yscrollcommand=scrollbar.set)
        
        # Status
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(main_frame, textvariable=self.status_var).grid(row=6, column=0, columnspan=3, pady=5)
    
    def select_input_folder(self):
        folder = filedialog.askdirectory(title="Select folder with exported chats")
        if folder:
            self.input_folder_var.set(folder)
    
    def select_output_folder(self):
        folder = filedialog.askdirectory(title="Select output folder")
        if folder:
            self.output_folder_var.set(folder)
    
    def log_callback(self, message):
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, message + '\n')
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        self.root.update()
    
    def start_processing(self):
        if not self.input_folder_var.get():
            messagebox.showerror("Error", "Please select an input folder")
            return
        
        if not self.output_folder_var.get():
            messagebox.showerror("Error", "Please select an output folder")
            return
        
        config.html_chat_folder = self.input_folder_var.get()
        config.output_folder = self.output_folder_var.get()
        
        global logger
        logger = ProcessingLogger(os.path.join(config.output_folder, 'processing_log.txt'))
        logger.gui_callback = self.log_callback
        
        self.status_var.set("Processing...")
        
        # Run in separate thread to prevent GUI freeze
        thread = threading.Thread(target=self.process_thread)
        thread.start()
    
    def process_thread(self):
        try:
            success = process_chat_exports(logger)
            if success:
                self.status_var.set("Processing Complete!")
                messagebox.showinfo("Success", "Processing completed successfully!")
            else:
                self.status_var.set("Processing Failed")
                messagebox.showerror("Error", "Processing failed. Check the log for details.")
        except Exception as e:
            self.status_var.set("Processing Failed")
            messagebox.showerror("Error", f"An error occurred: {str(e)}")

# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    logger = ProcessingLogger('processing_log.txt')
    root = tk.Tk()
    app = TeamsChatProcessorGUI(root)
    root.mainloop()