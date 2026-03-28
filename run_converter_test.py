"""
CLI Runner for Teams Chat Converter

Usage:
    python run_converter_test.py <input_folder> [-o OUTPUT_DIR] [-r] [--no-combine]

Examples:
    python run_converter_test.py path/to/exports
    python run_converter_test.py path/to/exports -o path/to/output
    python run_converter_test.py path/to/exports -r
    python run_converter_test.py path/to/exports --no-combine
"""

import argparse
import sys

from teams_chat_converter import convert_teams_chat_folder


def main():
    parser = argparse.ArgumentParser(
        description="Convert Teams chat HTML exports to Excel."
    )
    parser.add_argument(
        "input_folder",
        help="Path to the folder containing Teams chat HTML files.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=None,
        help="Directory where output files are written. Defaults to the input folder.",
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Search for HTML files recursively in sub-folders.",
    )
    parser.add_argument(
        "--no-combine",
        action="store_true",
        help="Write a separate Excel file per HTML file instead of one combined workbook.",
    )

    args = parser.parse_args()

    print(f"Starting Teams Chat conversion...")
    print(f"Input folder: {args.input_folder}")

    try:
        excel_file, log_file = convert_teams_chat_folder(
            args.input_folder,
            output_dir=args.output_dir,
            recursive=args.recursive,
            combine=not args.no_combine,
        )

        print("Conversion complete.")
        print(f"Excel output: {excel_file}")
        print(f"Log file:     {log_file}")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()