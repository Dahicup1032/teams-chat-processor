#!/usr/bin/env python3
"""
CLI runner for Teams Chat Converter.

Purpose:
- Provide a manual/command-line way to run conversions against test Purview Teams HTML exports,
  using the same conversion pipeline the GUI/executable uses:
    parse_html -> remove_duplicates -> check_timestamp_drift -> save_to_excel

Usage examples:
  # Single file (output goes next to input by default)
  python teams_chat_converter_cli.py --input "C:\cases\test1.html"

  # Folder of HTML files (non-recursive)
  python teams_chat_converter_cli.py --input "C:\cases\exports"

  # Folder recursive + explicit output directory
  python teams_chat_converter_cli.py --input "C:\cases\exports" --recursive --output "C:\cases\out"

Exit codes:
  0 = all conversions succeeded
  2 = some conversions failed
  3 = input path not found / invalid
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Tuple

# Import converter with a fallback pattern (mirrors GUI behavior)
try:
    from teams_chat_converter import TeamsChartConverter
except ImportError:  # pragma: no cover
    import teams_chat_converter  # type: ignore
    TeamsChartConverter = teams_chat_converter.TeamsChartConverter  # type: ignore


def iter_html_files(input_path: Path, recursive: bool) -> List[Path]:
    if input_path.is_file():
        return [input_path]

    patterns = ["*.html", "*.htm"]
    files: List[Path] = []
    for pat in patterns:
        files.extend(input_path.rglob(pat) if recursive else input_path.glob(pat))

    # Deduplicate and sort for deterministic processing order
    return sorted({p.resolve() for p in files})


def convert_one(html_file: Path, output_dir: Path | None, quiet: bool) -> Tuple[bool, str]:
    """
    Convert one HTML file -> Excel using the same steps as the GUI.
    Returns: (success, message)
    """
    try:
        # If output_dir is None, converter defaults to same folder as input (GUI behavior)
        converter = TeamsChartConverter(str(html_file), str(output_dir) if output_dir else None)

        if not quiet:
            print(f"\n=== Converting: {html_file} ===")

        df = converter.parse_html()

        # The GUI calls these methods; we mirror that here
        df = converter.remove_duplicates(df)
        df = converter.check_timestamp_drift(df)

        excel_file = converter.save_to_excel(df)

        if not quiet:
            stats = getattr(converter, "stats", {})
            log_file = getattr(converter, "log_file", None)
            print(f"✓ Excel created: {excel_file}")
            if log_file:
                print(f"✓ Log file:     {log_file}")
            if isinstance(stats, dict) and stats:
                # Print a small, stable subset
                print(
                    "✓ Stats: "
                    f"messages={len(df):,}, "
                    f"dupes_removed={stats.get('duplicates_removed', 'NA')}, "
                    f"drifts={stats.get('timestamp_drifts', 'NA')}, "
                    f"urls={stats.get('urls_extracted', 'NA')}, "
                    f"attachments={stats.get('attachments_found', 'NA')}"
                )

        return True, str(excel_file)

    except Exception as e:
        msg = f"FAILED: {html_file} :: {e}"
        if not quiet:
            print(msg)
        return False, msg


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="teams_chat_converter_cli",
        description="Convert Purview Teams Chat HTML exports to Excel (CLI runner for Teams Chat Converter).",
    )
    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to a Purview HTML export file OR a folder containing .html/.htm files.",
    )
    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help=(
            "Output directory for generated Excel/log files. "
            "If omitted, output is created next to each input file (matches GUI behavior)."
        ),
    )
    parser.add_argument(
        "--recursive",
        "-r",
        action="store_true",
        help="If --input is a folder, search recursively for .html/.htm files.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Reduce console output (conversion still logs via converter).",
    )

    args = parser.parse_args(argv)

    input_path = Path(args.input).expanduser()
    if not input_path.exists():
        print(f"ERROR: Input path not found: {input_path}")
        return 3

    output_dir = Path(args.output).expanduser() if args.output else None
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    html_files = iter_html_files(input_path, args.recursive)
    if not html_files:
        print(f"ERROR: No .html/.htm files found under: {input_path}")
        return 3

    failures: List[str] = []
    successes: List[str] = []

    if not args.quiet:
        print(f"Found {len(html_files)} HTML file(s) to process.")

    for f in html_files:
        ok, info = convert_one(f, output_dir, args.quiet)
        if ok:
            successes.append(info)
        else:
            failures.append(info)

    if not args.quiet:
        print("\n" + "=" * 70)
        print("DONE")
        print(f"Successful: {len(successes)}")
        print(f"Failed:     {len(failures)}")
        if failures:
            print("\nFailures:")
            for line in failures:
                print(f" - {line}")
        print("=" * 70)

    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())