#!/usr/bin/env python3
"""
CLI runner for Teams Chat Converter.

Purpose:
- Provide a manual/command-line way to run conversions against test Purview Teams HTML exports,
  using the same conversion pipeline the GUI/executable uses:
    parse_html -> remove_duplicates -> check_timestamp_drift -> save_to_excel
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Tuple

# ✅ FIXED CLASS NAME
try:
    from teams_chat_converter import TeamsChatConverter
except ImportError:  # pragma: no cover
    import teams_chat_converter  # type: ignore
    TeamsChatConverter = teams_chat_converter.TeamsChatConverter  # type: ignore


def iter_html_files(input_path: Path, recursive: bool) -> List[Path]:
    """Return list of HTML files from file or folder input."""
    if input_path.is_file():
        # ✅ Ensure it's actually an HTML file
        if input_path.suffix.lower() in {".html", ".htm"}:
            return [input_path]
        return []

    patterns = ["*.html", "*.htm"]
    files: List[Path] = []

    for pat in patterns:
        if recursive:
            files.extend(input_path.rglob(pat))
        else:
            files.extend(input_path.glob(pat))

    # Deduplicate and sort
    return sorted({p.resolve() for p in files})


def convert_one(html_file: Path, output_dir: Path | None, quiet: bool) -> Tuple[bool, str]:
    """Convert one HTML file to Excel."""
    try:
        converter = TeamsChatConverter(
            str(html_file),
            str(output_dir) if output_dir else None
        )

        if not quiet:
            print(f"\n=== Converting: {html_file} ===")

        df = converter.parse_html()

        # ✅ KEEP FULL PIPELINE
        df = converter.remove_duplicates(df)
        df = converter.check_timestamp_drift(df)

        excel_file = converter.save_to_excel(df)

        if not quiet:
            stats = getattr(converter, "stats", {})
            log_file = getattr(converter, "log_file", None)

            print(f"✓ Excel created: {excel_file}")
            if log_file:
                print(f"✓ Log file:     {log_file}")

            if isinstance(stats, dict):
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
        description="Convert Purview Teams Chat HTML exports to Excel.",
    )

    parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to a Purview HTML export file OR a folder containing HTML files.",
    )

    parser.add_argument(
        "--output",
        "-o",
        default=None,
        help="Output directory (defaults to same folder as input file).",
    )

    parser.add_argument(
        "--recursive",
        "-r",
        action="store_true",
        help="Search subfolders for HTML files.",
    )

    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Reduce console output.",
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

    if not args.quiet:
        print(f"Found {len(html_files)} HTML file(s) to process.")

    failures: List[str] = []
    successes: List[str] = []

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
