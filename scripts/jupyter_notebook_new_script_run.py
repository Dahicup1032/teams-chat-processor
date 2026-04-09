from pathlib import Path

# CHANGE THESE
REPO_DIR = Path(r"C:\Users\YourName\Documents\teams-chat-processor")
INPUT_PATH = Path(r"C:\cases\exports")   # can be a single .html file OR a folder
OUTPUT_DIR = Path(r"C:\cases\out")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

print("Repo:", REPO_DIR)
print("Input:", INPUT_PATH)
print("Output:", OUTPUT_DIR)

#Cell 2

import py_compile

parser_file = REPO_DIR / "teams_chat_converter.py"

try:
    py_compile.compile(str(parser_file), doraise=True)
    print("Syntax check passed:", parser_file)
except Exception as e:
    print("Syntax check FAILED:")
    print(e)
	
#Cell 3

import sys
import importlib.util

parser_file = REPO_DIR / "teams_chat_converter.py"

spec = importlib.util.spec_from_file_location("teams_chat_converter", parser_file)
teams_chat_converter = importlib.util.module_from_spec(spec)
spec.loader.exec_module(teams_chat_converter)

TeamsChatConverter = teams_chat_converter.TeamsChatConverter
convert_teams_chat = teams_chat_converter.convert_teams_chat

print("Imported parser OK")
print("Class:", TeamsChatConverter)
print("Function:", convert_teams_chat)

#Cell 4

def iter_html_files(input_path: Path, recursive: bool = False):
    if input_path.is_file():
        if input_path.suffix.lower() in {".html", ".htm"}:
            return [input_path]
        return []

    patterns = ["*.html", "*.htm"]
    files = []
    for pat in patterns:
        files.extend(input_path.rglob(pat) if recursive else input_path.glob(pat))

    return sorted({p.resolve() for p in files})
	
	
#Cell 5

def run_parser_notebook(input_path, output_dir, recursive=False):
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    html_files = iter_html_files(input_path, recursive=recursive)
    if not html_files:
        raise FileNotFoundError(f"No HTML files found under: {input_path}")

    results = []
    failures = []

    print(f"Found {len(html_files)} HTML file(s)")

    for html_file in html_files:
        print(f"\n=== Processing: {html_file} ===")
        try:
            converter = TeamsChatConverter(str(html_file), str(output_dir))

            df = converter.parse_html()
            df = converter.remove_duplicates(df)
            df = converter.check_timestamp_drift(df)
            excel_file = converter.save_to_excel(df)

            stats = getattr(converter, "stats", {})
            log_file = getattr(converter, "log_file", None)

            print("Excel:", excel_file)
            print("Log:  ", log_file)
            print(
                "Stats:",
                {
                    "messages": len(df),
                    "duplicates_removed": stats.get("duplicates_removed"),
                    "timestamp_drifts": stats.get("timestamp_drifts"),
                    "urls_extracted": stats.get("urls_extracted"),
                    "attachments_found": stats.get("attachments_found"),
                }
            )

            results.append({
                "html_file": str(html_file),
                "excel_file": str(excel_file),
                "log_file": str(log_file) if log_file else "",
                "messages": len(df),
                "duplicates_removed": stats.get("duplicates_removed"),
                "timestamp_drifts": stats.get("timestamp_drifts"),
                "urls_extracted": stats.get("urls_extracted"),
                "attachments_found": stats.get("attachments_found"),
            })

        except Exception as e:
            print("FAILED:", e)
            failures.append({"html_file": str(html_file), "error": str(e)})

    return results, failures
	
	
#Cell 6-Execute

#Single File

results, failures = run_parser_notebook(
    input_path=INPUT_PATH,   # point INPUT_PATH to one .html file
    output_dir=OUTPUT_DIR,
    recursive=False
)

# Folder

results, failures = run_parser_notebook(
    input_path=INPUT_PATH,   # point INPUT_PATH to a folder
    output_dir=OUTPUT_DIR,
    recursive=False
)

# Folder & Sub Folder

results, failures = run_parser_notebook(
    input_path=INPUT_PATH,
    output_dir=OUTPUT_DIR,
    recursive=True
)

#Cell 7-Show Summary

import pandas as pd

results_df = pd.DataFrame(results)
failures_df = pd.DataFrame(failures)

print("Successful:", len(results_df))
print("Failed:", len(failures_df))

display(results_df)

if not failures_df.empty:
    display(failures_df)
	
	
