"""GUI for Teams Chat Converter - Browse-only version"""
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
from pathlib import Path
import sys
import threading

try:
    from teams_chat_converter import TeamsChartConverter
except ImportError:
    import teams_chat_converter
    TeamsChartConverter = teams_chat_converter.TeamsChartConverter


class ConverterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Teams Chat Converter")
        self.root.geometry("700x500")
        self.root.resizable(True, True)
        self.selected_file = None
        self.is_converting = False
        self.setup_ui()
        
    def setup_ui(self):
        title_label = tk.Label(self.root, text="Purview Teams Chat Converter", font=("Arial", 16, "bold"), pady=10)
        title_label.pack()
        
        instruction_label = tk.Label(self.root, text="Click 'Browse' to select your Purview HTML export file", font=("Arial", 10), pady=5)
        instruction_label.pack()
        
        self.file_frame = tk.Frame(self.root, relief=tk.SUNKEN, borderwidth=2, bg="#f0f0f0", height=100)
        self.file_frame.pack(padx=20, pady=10, fill=tk.BOTH, expand=False)
        
        self.file_label = tk.Label(self.file_frame, text="No file selected", font=("Arial", 10), bg="#f0f0f0", fg="#666666", wraplength=600)
        self.file_label.pack(expand=True, pady=20)
        
        self.browse_button = tk.Button(self.root, text="Browse...", command=self.browse_file, font=("Arial", 11), width=15, height=2)
        self.browse_button.pack(pady=10)
        
        self.convert_button = tk.Button(self.root, text="Convert to Excel", command=self.start_conversion, font=("Arial", 12, "bold"), bg="#0078d4", fg="white", width=20, height=2, state=tk.DISABLED)
        self.convert_button.pack(pady=10)
        
        status_label = tk.Label(self.root, text="Processing Log:", font=("Arial", 10, "bold"), anchor=tk.W)
        status_label.pack(padx=20, pady=(10, 5), anchor=tk.W)
        
        self.status_text = scrolledtext.ScrolledText(self.root, height=10, width=80, font=("Courier", 9), state=tk.DISABLED, bg="#ffffff")
        self.status_text.pack(padx=20, pady=(0, 10), fill=tk.BOTH, expand=True)
        
        self.log_message("Ready. Please select an HTML file to convert.")
        
    def browse_file(self):
        if self.is_converting:
            return
        file_path = filedialog.askopenfilename(title="Select Purview HTML Export", filetypes=[("HTML files", "*.html *.htm"), ("All files", "*.*")])
        if file_path:
            self.select_file(file_path)
    
    def select_file(self, file_path):
        file_path = Path(file_path)
        if not file_path.exists():
            self.log_message(f"ERROR: File not found: {file_path}")
            return
        if file_path.suffix.lower() not in ['.html', '.htm']:
            response = messagebox.askyesno("Non-HTML File", "This doesn't appear to be an HTML file. Continue anyway?")
            if not response:
                return
        self.selected_file = file_path
        self.file_label.config(text=f"Selected: {file_path.name}\n\nPath: {file_path}", fg="#000000")
        self.convert_button.config(state=tk.NORMAL)
        self.log_message(f"File selected: {file_path.name}")
        
    def log_message(self, message):
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)
        self.root.update_idletasks()
        
    def start_conversion(self):
        if not self.selected_file or self.is_converting:
            return
        self.is_converting = True
        self.convert_button.config(state=tk.DISABLED, text="Converting...")
        self.browse_button.config(state=tk.DISABLED)
        self.log_message("\n" + "="*60)
        self.log_message("Starting conversion...")
        self.log_message("="*60)
        thread = threading.Thread(target=self.run_conversion, daemon=True)
        thread.start()
        
    def run_conversion(self):
        try:
            converter = TeamsChartConverter(str(self.selected_file), str(self.selected_file.parent))
            self.log_message(f"Input file: {self.selected_file}")
            self.log_message("Parsing HTML...")
            df = converter.parse_html()
            self.log_message(f"✓ Extracted {len(df)} messages")
            self.log_message("Checking for duplicates...")
            df = converter.remove_duplicates(df)
            self.log_message(f"✓ Removed {converter.stats['duplicates_removed']} duplicates")
            self.log_message("Checking timestamp drift...")
            df = converter.check_timestamp_drift(df)
            self.log_message(f"✓ Detected {converter.stats['timestamp_drifts']} timestamp drifts")
            self.log_message("Saving to Excel...")
            excel_file = converter.save_to_excel(df)
            self.log_message(f"✓ Excel file created: {Path(excel_file).name}")
            self.log_message("\n" + "="*60)
            self.log_message("CONVERSION COMPLETE!")
            self.log_message("="*60)
            self.log_message(f"Total messages: {len(df):,}")
            self.log_message(f"URLs extracted: {converter.stats['urls_extracted']:,}")
            self.log_message(f"Attachments found: {converter.stats['attachments_found']:,}")
            self.log_message(f"\nOutput files:")
            self.log_message(f"  Excel: {excel_file}")
            self.log_message(f"  Log: {converter.log_file}")
            self.log_message("="*60)
            self.root.after(0, lambda: self.show_success_dialog(excel_file))
        except Exception as e:
            error_msg = f"ERROR: Conversion failed: {str(e)}"
            self.log_message(error_msg)
            import traceback
            self.log_message(traceback.format_exc())
            self.root.after(0, lambda: messagebox.showerror("Conversion Failed", error_msg))
        finally:
            self.root.after(0, self.reset_ui)
    
    def show_success_dialog(self, excel_file):
        response = messagebox.askyesno("Conversion Complete", f"Conversion completed successfully!\n\nExcel file: {Path(excel_file).name}\n\nOpen output folder?", icon=messagebox.INFO)
        if response:
            import os
            folder = Path(excel_file).parent
            try:
                if sys.platform == 'win32':
                    os.startfile(folder)
            except Exception as e:
                messagebox.showwarning("Cannot Open Folder", f"Could not open folder: {e}")
    
    def reset_ui(self):
        self.is_converting = False
        self.convert_button.config(state=tk.NORMAL, text="Convert to Excel")
        self.browse_button.config(state=tk.NORMAL)


def main():
    root = tk.Tk()
    app = ConverterGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()