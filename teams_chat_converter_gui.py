"""GUI for Teams Chat Converter - single file and folder mode"""

from __future__ import annotations

import os
import sys
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

try:
    from teams_chat_converter import convert_teams_chat, convert_teams_chat_folder
except ImportError:  # pragma: no cover
    import teams_chat_converter  # type: ignore
    convert_teams_chat = teams_chat_converter.convert_teams_chat  # type: ignore
    convert_teams_chat_folder = teams_chat_converter.convert_teams_chat_folder  # type: ignore


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
                "Single file mode creates one Excel workbook. Folder mode creates one combined Excel workbook."
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

        if self.mode_var.get() == "file" and path.suffix.lower() not in [".html", ".htm"]:
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
            input_path = self.selected_path
            output_dir = str(self.output_dir) if self.output_dir else None

            if input_path is None:
                raise ValueError("No input selected.")

            results = []
            failures = []

            if self.mode_var.get() == "file":
                self.log_message(f"Processing single file: {input_path.name}")
                excel_file, log_file = convert_teams_chat(
                    str(input_path),
                    output_dir=output_dir,
                )
                self.log_message(f"✓ Excel file created: {Path(excel_file).name}")
                self.log_message(f"✓ Log file: {Path(log_file).name}")
                results.append(str(excel_file))

            else:
                self.log_message(f"Processing folder: {input_path}")
                self.log_message("Combining all HTML files into one Excel workbook...")
                excel_file, log_file = convert_teams_chat_folder(
                    str(input_path),
                    output_dir=output_dir,
                    recursive=self.recursive_var.get(),
                    combine=True,
                )
                self.log_message(f"✓ Combined Excel file created: {Path(excel_file).name}")
                self.log_message(f"✓ Log file: {Path(log_file).name}")
                results.append(str(excel_file))

            self.log_message("\n" + "=" * 70)
            self.log_message("CONVERSION COMPLETE")
            self.log_message(f"Successful: {len(results)}")
            self.log_message(f"Failed: {len(failures)}")
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
