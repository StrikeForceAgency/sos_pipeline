"""
Main pipeline entry point for the SOS lead generation system.

This script orchestrates the extraction/renaming and enrichment steps of the
pipeline.  By default both stages will run sequentially, but callers can
selectively invoke only a subset of the pipeline via command‑line flags.

Usage:

    python3 pipeline.py                # run both extraction and enrichment
    python3 pipeline.py --extract-only  # only unpack ZIPs and rename CSVs
    python3 pipeline.py --enrich-only   # only run enrichment on existing CSVs

Flags cannot be combined; if both ``--extract-only`` and ``--enrich-only``
are specified the program will exit with an error.
"""

from __future__ import annotations

import argparse
import sys

from scripts.extract_and_rename import extract_and_rename
from scripts.enrichment import enrich_and_export


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="SOS Pipeline Runner")
    parser.add_argument(
        "--extract-only",
        action="store_true",
        help="Only run the extraction and renaming step",
    )
    parser.add_argument(
        "--enrich-only",
        action="store_true",
        help="Only run the enrichment step (assumes extracted CSVs are present)",
    )
    args = parser.parse_args(argv)

    if args.extract_only and args.enrich_only:
        parser.error("--extract-only and --enrich-only cannot be used together.")

    if args.extract_only:
        extract_and_rename()
        return

    if args.enrich_only:
        enrich_and_export()
        return

    # Default behaviour: run both
    extract_and_rename()
    enrich_and_export()


if __name__ == "__main__":
    main(sys.argv[1:])
