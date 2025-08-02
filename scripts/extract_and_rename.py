"""
Extraction and renaming module for the SOS pipeline.

This script unpacks all ZIP archives found in the ``raw_zips/`` directory,
regardless of nesting depth, and moves the contained CSV files into the
``raw/`` directory with standardized names.  Known CSV filenames (Filings.csv,
Agents.csv, Principals.csv) are renamed to include a datestamp, while
unknown CSVs are slugified and timestamped.  Duplicate filenames are avoided
by appending suffixes (e.g. ``_1``, ``_2``).  After processing each ZIP file
it is moved into ``archive/raw_zips/`` and a log of the run is saved into
``logs/``.

This module is adapted from the user's existing implementation and keeps the
same behaviour.  It is separated here to allow import into the orchestrating
pipeline without invoking top‑level code.
"""

from __future__ import annotations

import datetime
import shutil
import zipfile
from pathlib import Path
from typing import Dict, List

# Determine base directory relative to this file (../.. -> project root)
_BASE_DIR = Path(__file__).resolve().parents[1]

# Paths relative to the project root.  Using absolute paths derived from
# ``__file__`` ensures consistent behaviour regardless of the current
# working directory when this module is invoked.
RAW_ZIPS_DIR = _BASE_DIR / "raw_zips"
RAW_DIR = _BASE_DIR / "raw"
ARCHIVE_DIR = _BASE_DIR / "archive" / "raw_zips"
LOG_DIR = _BASE_DIR / "logs"

timestamp = datetime.datetime.now().strftime("%Y-%m-%d")

FILENAME_MAP: Dict[str, str] = {
    "Filings.csv": f"Filings_{timestamp}.csv",
    "Agents.csv": f"Agents_{timestamp}.csv",
    "Principals.csv": f"Principals_{timestamp}.csv",
}


def safe_target_path(target_dir: Path, base_name: str) -> Path:
    """Return a target path that does not clobber existing files by appending
    numerical suffixes to the base name if necessary."""
    target = target_dir / base_name
    i = 1
    while target.exists():
        name_parts = base_name.split(".")
        name_parts[0] += f"_{i}"
        target = target_dir / ".".join(name_parts)
        i += 1
    return target


def slugify(name: str) -> str:
    """Create a filesystem‑friendly version of the filename without the extension."""
    return (
        name.replace(" ", "_")
        .replace(".csv", "")
        .replace("-", "_")
    )


def extract_and_rename() -> None:
    """Perform the extraction and renaming of all ZIP archives in RAW_ZIPS_DIR."""
    renamed_count = 0
    skipped_zips: List[str] = []
    renamed_files: List[str] = []
    log_lines: List[str] = []

    print("Running SOS Pipeline...")

    for zip_file in RAW_ZIPS_DIR.glob("*.zip"):
        print(f"📦 Extracting: {zip_file.name}")
        log_lines.append(f"[ZIP] Extracting: {zip_file.name}")

        tmp_dir = RAW_DIR / f"tmp_{zip_file.stem}"
        tmp_dir.mkdir(parents=True, exist_ok=True)

        try:
            with zipfile.ZipFile(zip_file, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)
        except zipfile.BadZipFile:
            error = f"❌ Corrupted ZIP: {zip_file.name}"
            print(error)
            log_lines.append(f"[ERROR] {error}")
            skipped_zips.append(zip_file.name)
            continue

        csv_files = list(tmp_dir.rglob("*.csv"))

        if not csv_files:
            warning = f"⚠️ No .csv files found in {zip_file.name}"
            print(warning)
            log_lines.append(f"[WARN] {warning}")
            skipped_zips.append(zip_file.name)
        else:
            for file in csv_files:
                base_name = file.name
                new_name = (
                    FILENAME_MAP.get(base_name)
                    or f"{slugify(base_name)}_{timestamp}.csv"
                )
                target = safe_target_path(RAW_DIR, new_name)
                shutil.move(str(file), target)
                print(f"✅ {base_name} → {target.name}")
                log_lines.append(f"[RENAMED] {base_name} → {target.name}")
                renamed_files.append(target.name)
                renamed_count += 1

        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.move(str(zip_file), ARCHIVE_DIR / zip_file.name)
        shutil.rmtree(tmp_dir, ignore_errors=True)

    print("\n🧾 Pipeline Summary:")
    print(f"✔️ Renamed files: {renamed_count}")
    print(f"📄 Files: {renamed_files}")
    print(f"🚫 Skipped ZIPs: {len(skipped_zips)}")

    log_lines.append("\n--- SUMMARY ---")
    log_lines.append(f"✔️ Total renamed: {renamed_count}")
    log_lines.append(f"🚫 Skipped ZIPs: {len(skipped_zips)}")
    log_lines.extend([f" - {name}" for name in skipped_zips])

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"extract_log_{timestamp}.log"
    with open(log_file, "w") as f:
        f.write("\n".join(log_lines))

    print(f"\n📝 Log saved to: {log_file}")
    print("🎉 Extraction and renaming complete.")


if __name__ == "__main__":  # pragma: no cover
    extract_and_rename()
