"""GUI for Teams Chat Converter - single file and folder mode"""

from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

try:
    from teams_chat_converter import TeamsChatConverter
except ImportError:  # pragma: no cover
    import teams_chat_converter  # type: ignore
    TeamsChatConverter = teams_chat_converter.TeamsChatConverter  # type: ignore


class ConverterGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Teams Chat Converter")
        self.root.geometry("860x650")
        self.root.resizable(True, True)

        self.selected_path: Path | None = None
        self.output_dir: Path | None = None
        self.is_converting = False

        self.mode_var = tk.StringVar(value="file")
        self.recursive_var = tk.BooleanVar(value=False)

        self.setup_ui()

    def setup_ui(self):
        title_label = tk.Label(
            self.root,
            text="Purview Teams Chat Converter",
            font=("Arial", 16, "bold"),
            pady=10,
        )
        title_label.pack()

        instruction_label = tk.Label(
            self.root,
            text=(
                "Select either a single Purview HTML export file or a folder of HTML files. "
                "The converter extracts message IDs, sender, timestamps, message text, "
                "conversation participants, URLs, attachments, and timestamp drift results."
            ),
            font=("Arial", 10),
            pady=5,
            wraplength=800,
            justify="center",
        )
        instruction_label.pack()

        mode_frame = tk.LabelFrame(self.root, text="Input Mode", padx=10, pady=8)
        mode_frame.pack(fill=tk.X, padx=20, pady=(10, 5))

        tk.Radiobutton(
            mode_frame,
            text="Single HTML file",
            variable=self.mode_var,
            value="file",
            command=self.on_mode_change,
        ).pack(side=tk.LEFT, padx=(5, 20))

        tk.Radiobutton(
            mode_frame,
            text="Folder of HTML files",
            variable=self.mode_var,
            value="folder",
            command=self.on_mode_change,
        ).pack(side=tk.LEFT, padx=(5, 20))

        self.recursive_check = tk.Checkbutton(
            mode_frame,
            text="Include subfolders (recursive)",
            variable=self.recursive_var,
        )
        self.recursive_check.pack(side=tk.LEFT, padx=(5, 20))

        path_frame = tk.Frame(self.root)
        path_frame.pack(fill=tk.X, padx=20, pady=5)

        self.path_display = tk.Label(
            path_frame,
            text="No file or folder selected",
            font=("Arial", 10),
            anchor="w",
            justify="left",
            relief=tk.SUNKEN,
            bd=2,
            bg="#f0f0f0",
            fg="#666666",
            wraplength=800,
            padx=10,
            pady=12,
        )
        self.path_display.pack(fill=tk.X)

        button_frame = tk.Frame(self.root)
        button_frame.pack(fill=tk.X, padx=20, pady=10)

        self.browse_button = tk.Button(
            button_frame,
            text="Browse...",
            command=self.browse_input,
            font=("Arial", 11),
            width=16,
            height=2,
        )
        self.browse_button.pack(side=tk.LEFT, padx=(0, 10))

        self.output_button = tk.Button(
            button_frame,
            text="Output Folder...",
            command=self.choose_output_dir,
            font=("Arial", 11),
            width=16,
            height=2,
        )
        self.output_button.pack(side=tk.LEFT, padx=(0, 10))

        self.convert_button = tk.Button(
            button_frame,
            text="Convert to Excel",
            command=self.start_conversion,
            font=("Arial", 12, "bold"),
            bg="#0078d4",
            fg="white",
            width=20,
            height=2,
            state=tk.DISABLED,
        )
        self.convert_button.pack(side=tk.LEFT, padx=(0, 10))

        self.output_label = tk.Label(
            self.root,
            text="Output: same folder as selected input",
            font=("Arial", 9),
            anchor="w",
            justify="left",
        )
        self.output_label.pack(fill=tk.X, padx=20, pady=(0, 8))

        status_label = tk.Label(
            self.root,
            text="Processing Log:",
            font=("Arial", 10, "bold"),
            anchor=tk.W,
        )
        status_label.pack(padx=20, pady=(10, 5), anchor=tk.W)

        self.status_text = scrolledtext.ScrolledText(
            self.root,
            height=18,
            width=100,
            font=("Courier", 9),
            state=tk.DISABLED,
            bg="#ffffff",
        )
        self.status_text.pack(padx=20, pady=(0, 10), fill=tk.BOTH, expand=True)

        self.on_mode_change()
        self.log_message("Ready.")
        self.log_message("Select a file or folder to begin.")

    def on_mode_change(self):
        if self.mode_var.get() == "file":
            self.recursive_check.config(state=tk.DISABLED)
        else:
            self.recursive_check.config(state=tk.NORMAL)

    def browse_input(self):
        if self.is_converting:
            return

        if self.mode_var.get() == "file":
            path = filedialog.askopenfilename(
                title="Select Purview HTML Export",
                filetypes=[("HTML files", "*.html *.htm"), ("All files", "*.*")],
            )
        else:
            path = filedialog.askdirectory(title="Select Folder Containing HTML Files")

        if path:
            self.select_input(path)

    def choose_output_dir(self):
        if self.is_converting:
            return

        path = filedialog.askdirectory(title="Select Output Folder")
        if path:
            self.output_dir = Path(path)
            self.output_label.config(text=f"Output: {self.output_dir}")
            self.log_message(f"Output folder selected: {self.output_dir}")

    def select_input(self, path_str: str):
        path = Path(path_str)

        if not path.exists():
            self.log_message(f"ERROR: Path not found: {path}")
            return

        if self.mode_var.get() == "file":
            if path.suffix.lower() not in [".html", ".htm"]:
                response = messagebox.askyesno(
                    "Non-HTML File",
                    "This does not appear to be an HTML file.\nContinue anyway?",
                )
                if not response:
                    return

        self.selected_path = path
        self.path_display.config(
            text=f"Selected: {path.name}\n\nPath: {path}",
            fg="#000000",
        )
        self.convert_button.config(state=tk.NORMAL)
        self.log_message(f"Input selected: {path}")

    def iter_html_files(self, input_path: Path, recursive: bool):
        if input_path.is_file():
            if input_path.suffix.lower() in {".html", ".htm"}:
                return [input_path]
            return []

        patterns = ["*.html", "*.htm"]
        files = []
        for pat in patterns:
            files.extend(input_path.rglob(pat) if recursive else input_path.glob(pat))

        return sorted({p.resolve() for p in files})

    def log_message(self, message: str):
        self.status_text.config(state=tk.NORMAL)
        self.status_text.insert(tk.END, message + "\n")
        self.status_text.see(tk.END)
        self.status_text.config(state=tk.DISABLED)
        self.root.update_idletasks()

    def start_conversion(self):
        if not self.selected_path or self.is_converting:
            return

        self.is_converting = True
        self.convert_button.config(state=tk.DISABLED, text="Converting...")
        self.browse_button.config(state=tk.DISABLED)
        self.output_button.config(state=tk.DISABLED)

        self.log_message("\n" + "=" * 70)
        self.log_message("Starting conversion...")
        self.log_message("=" * 70)

        thread = threading.Thread(target=self.run_conversion, daemon=True)
        thread.start()
   
    def run_conversion(self):
        try:
            import pandas as pd

            input_path = self.selected_path
            output_dir = self.output_dir

            if input_path is None:
                raise ValueError("No input selected.")

            html_files = self.iter_html_files(
                input_path,
                recursive=self.recursive_var.get() if self.mode_var.get() == "folder" else False,
            )

            if not html_files:
                raise FileNotFoundError(f"No .html/.htm files found under: {input_path}")

            self.log_message(f"Found {len(html_files)} HTML file(s) to process.")

            results = []
            failures = []

            # SINGLE FILE MODE = keep existing one-file behavior
            if self.mode_var.get() == "file":
                html_file = html_files[0]
                try:
                    effective_output = output_dir if output_dir else html_file.parent
                    converter = TeamsChatConverter(str(html_file), str(effective_output))

                    self.log_message(f"\n--- Processing: {html_file.name} ---")
                    self.log_message("Parsing HTML...")
                    df = converter.parse_html()
                    self.log_message(f"✓ Extracted {len(df)} message rows")

                    self.log_message("Checking for duplicates...")
                    df = converter.remove_duplicates(df)
                    self.log_message(f"✓ Removed {converter.stats['duplicates_removed']} duplicates")

                    self.log_message("Checking timestamp drift...")
                    df = converter.check_timestamp_drift(df)
                    self.log_message(f"✓ Detected {converter.stats['timestamp_drifts']} timestamp drifts")

                    self.log_message("Saving to Excel...")
                    excel_file = converter.save_to_excel(df)

                    self.log_message(f"✓ Excel file created: {Path(excel_file).name}")
                    self.log_message(f"  URLs extracted: {converter.stats['urls_extracted']}")
                    self.log_message(f"  Attachments found: {converter.stats['attachments_found']}")
                    self.log_message(f"  Log file: {converter.log_file}")

                    results.append(str(excel_file))

                except Exception as e:
                    msg = f"{html_file.name}: {e}"
                    failures.append(msg)
                    self.log_message(f"ERROR: {msg}")

            # FOLDER MODE = combine all HTMLs into one Excel
            else:
                all_dfs = []
                total_dupes = 0
                total_drifts = 0
                total_urls = 0
                total_attachments = 0

                effective_output = output_dir if output_dir else input_path

                for html_file in html_files:
                    try:
                        converter = TeamsChatConverter(str(html_file), str(effective_output))

                        self.log_message(f"\n--- Processing: {html_file.name} ---")
                        self.log_message("Parsing HTML...")
                        df = converter.parse_html()
                        self.log_message(f"✓ Extracted {len(df)} message rows")

                        self.log_message("Checking for duplicates...")
                        df = converter.remove_duplicates(df)
                        self.log_message(f"✓ Removed {converter.stats['duplicates_removed']} duplicates")

                        self.log_message("Checking timestamp drift...")
                        df = converter.check_timestamp_drift(df)
                        self.log_message(f"✓ Detected {converter.stats['timestamp_drifts']} timestamp drifts")

                        all_dfs.append(df)

                        total_dupes += converter.stats.get("duplicates_removed", 0)
                        total_drifts += converter.stats.get("timestamp_drifts", 0)
                        total_urls += converter.stats.get("urls_extracted", 0)
                        total_attachments += converter.stats.get("attachments_found", 0)

                    except Exception as e:
                        msg = f"{html_file.name}: {e}"
                        failures.append(msg)
                        self.log_message(f"ERROR: {msg}")

                if all_dfs:
                    self.log_message("\nCombining all parsed rows into one Excel file...")
                    combined_df = pd.concat(all_dfs, ignore_index=True)

                    combined_name = "combined_teams_chat_output.xlsx"
                    combined_path = Path(effective_output) / combined_name

                    with pd.ExcelWriter(combined_path, engine="openpyxl") as writer:
                        combined_df.to_excel(writer, index=False, sheet_name="Messages")

                        summary_df = pd.DataFrame([
                            {"Metric": "Source Folder", "Value": str(input_path)},
                            {"Metric": "Files Processed", "Value": len(all_dfs)},
                            {"Metric": "Combined Rows", "Value": len(combined_df)},
                            {"Metric": "Duplicates Removed", "Value": total_dupes},
                            {"Metric": "Timestamp Drifts", "Value": total_drifts},
                            {"Metric": "URLs Extracted", "Value": total_urls},
                            {"Metric": "Attachments Found", "Value": total_attachments},
                        ])
                        summary_df.to_excel(writer, index=False, sheet_name="Summary")

                    self.log_message(f"✓ Combined Excel file created: {combined_path.name}")
                    results.append(str(combined_path))

            self.log_message("\n" + "=" * 70)
            self.log_message("CONVERSION COMPLETE")
            self.log_message(f"Successful: {len(results)}")
            self.log_message(f"Failed: {len(failures)}")
            if failures:
                self.log_message("Failures:")
                for failure in failures:
                    self.log_message(f" - {failure}")
            self.log_message("=" * 70)

            self.root.after(0, lambda: self.show_completion_dialog(results, failures))

        except Exception as e:
            error_msg = f"ERROR: Conversion failed: {e}"
            self.log_message(error_msg)
            import traceback
            self.log_message(traceback.format_exc())
            self.root.after(0, lambda: messagebox.showerror("Conversion Failed", error_msg))
        finally:
            self.root.after(0, self.reset_ui)
    
    def show_completion_dialog(self, results, failures):
        if results and not failures:
            message = (
                f"Conversion completed successfully.\n\n"
                f"Files created: {len(results)}\n\n"
                f"Open output folder?"
            )
            response = messagebox.askyesno("Conversion Complete", message, icon=messagebox.INFO)
            if response:
                folder = self.output_dir if self.output_dir else Path(results[0]).parent
                try:
                    if sys.platform == "win32":
                        os.startfile(folder)
                except Exception as e:
                    messagebox.showwarning("Cannot Open Folder", f"Could not open folder: {e}")
        elif results and failures:
            messagebox.showwarning(
                "Conversion Complete With Errors",
                f"Completed with some errors.\n\nSuccessful: {len(results)}\nFailed: {len(failures)}"
            )
        else:
            messagebox.showerror("Conversion Failed", "No files were converted successfully.")

    def reset_ui(self):
        self.is_converting = False
        self.convert_button.config(state=tk.NORMAL, text="Convert to Excel")
        self.browse_button.config(state=tk.NORMAL)
        self.output_button.config(state=tk.NORMAL)


def main():
    root = tk.Tk()
    app = ConverterGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
