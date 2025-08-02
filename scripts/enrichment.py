"""
Enrichment module for the California SOS filing pipeline.

This script reads the extracted CSVs produced by the `extract_and_rename` step,
cleans and normalizes their contents, applies simple lead‑scoring flags, and
exports enriched datasets back into the `outputs/` folder.  The goal of the
enrichment step is to prepare the data for downstream CRM or email outreach
systems with minimal human intervention.

Key behaviours:

* Reads any number of `Filings_YYYY‑MM‑DD*.csv`, `Agents_YYYY‑MM‑DD*.csv` and
  `Principals_YYYY‑MM‑DD*.csv` files from the `raw/` directory.  All files
  matching the current date are concatenated together into a single DataFrame
  per type.
* Standardizes column names to snake_case and strips whitespace from values.
* Merges the filings and agents data on the common entity number to allow
  cross‑record comparisons (e.g. to determine if the agent address differs
  from the business address).
* Applies three enrichment flags:

    - `agent_differs_from_business_address`: True if the agent mailing
      address does not exactly match the filing address.
    - `likely_residential_address`: True if the address contains clues like
      "apt", "unit", "#", or "po box", suggesting a residential or PO box
      location.
    - `has_personal_email`: True if an email address is present and appears
      to be from a generic provider like gmail.com, yahoo.com or hotmail.com.

* Assigns a simple tier classification (A, B or C) based on the above flags:

    - **A:** Both `agent_differs_from_business_address` and
      `likely_residential_address` are True.
    - **B:** Exactly one of the flags is True.
    - **C:** Neither flag is True.

* Writes three CSV files into `outputs/` named `A_leads_YYYY‑MM‑DD.csv`,
  `B_leads_YYYY‑MM‑DD.csv` and `C_leads_YYYY‑MM‑DD.csv`, containing the
  enriched filings joined with the agent record and associated flags.  A
  summary log of the enrichment run is also printed to stdout.
"""

from __future__ import annotations

import datetime
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd

# Determine the base directory relative to this file.
_BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = _BASE_DIR / "raw"
OUTPUT_DIR = _BASE_DIR / "outputs"


def _standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with lower‑case, snake_case column names."""
    new_cols: List[str] = []
    seen = {}
    for col in df.columns:
        clean = col.strip().lower().replace(" ", "_").replace("-", "_")
        if clean in seen:
            seen[clean] += 1
            clean = f"{clean}_{seen[clean]}"
        else:
            seen[clean] = 0
        new_cols.append(clean)
    df = df.copy()
    df.columns = new_cols
    # Strip whitespace from string values
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype(str).str.strip()
    return df


def _load_latest_csvs(prefix: str, date_str: str) -> pd.DataFrame:
    """Load all CSV files in ``RAW_DIR`` starting with ``prefix`` and matching
    ``date_str``.  Returns an empty DataFrame if none are found."""
    pattern = f"{prefix}_{date_str}"
    csv_files = sorted(RAW_DIR.glob(f"{pattern}*.csv"))
    if not csv_files:
        print(f"⚠️ No {prefix} CSVs found for {date_str}.")
        return pd.DataFrame()
    frames: List[pd.DataFrame] = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, dtype=str, low_memory=False)
            frames.append(df)
        except Exception as e:
            print(f"❌ Error reading {f}: {e}")
    if not frames:
        return pd.DataFrame()
    concat_df = pd.concat(frames, ignore_index=True)
    return _standardize_columns(concat_df)


def _apply_flags(joined: pd.DataFrame) -> pd.DataFrame:
    """Compute enrichment flags on the merged filings/agents DataFrame."""
    df = joined.copy()

    def normalize_addr(addr: str) -> str:
        if not isinstance(addr, str) or not addr:
            return ""
        return re.sub(r"[\s,\.]+", " ", addr.strip().lower())

    # Build full addresses
    business_addr_cols = [c for c in df.columns if "address" in c and not c.startswith("agent_")]
    agent_addr_cols = [c for c in df.columns if c.startswith("agent_") and "address" in c]
    df["business_full_address"] = df[business_addr_cols].fillna("").agg(" ".join, axis=1)
    df["agent_full_address"] = df[agent_addr_cols].fillna("").agg(" ".join, axis=1)
    df["agent_differs_from_business_address"] = df.apply(
        lambda row: normalize_addr(row["agent_full_address"]) != normalize_addr(row["business_full_address"]),
        axis=1,
    )

    # Detect residential clues in either business or agent address
    residential_pattern = re.compile(r"\b(apt|apartment|unit|suite|#|po box|p\.o\. box)\b", re.IGNORECASE)
    df["likely_residential_address"] = df.apply(
        lambda row: bool(residential_pattern.search(row["business_full_address"]) or
                         residential_pattern.search(row["agent_full_address"])),
        axis=1,
    )

    # Detect personal email domains
    email_cols = [c for c in df.columns if "email" in c]
    consumer_domains = ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com")
    def is_personal(email: str) -> bool:
        if not isinstance(email, str) or "@" not in email:
            return False
        domain = email.split("@")[-1].lower()
        return any(domain.endswith(d) for d in consumer_domains)
    if email_cols:
        df["has_personal_email"] = df[email_cols].fillna("").apply(
            lambda row: any(is_personal(val) for val in row), axis=1
        )
    else:
        df["has_personal_email"] = False

    # Tier classification
    def classify(row) -> str:
        flags = row["agent_differs_from_business_address"], row["likely_residential_address"]
        if all(flags):
            return "A"
        if any(flags):
            return "B"
        return "C"
    df["lead_tier"] = df.apply(classify, axis=1)

    return df


def enrich_and_export() -> None:
    """Orchestrate the enrichment process."""
    date_str = datetime.datetime.now().strftime("%Y-%m-%d")

    filings_df = _load_latest_csvs("Filings", date_str)
    agents_df  = _load_latest_csvs("Agents",  date_str)
    # Principals could be loaded similarly but are unused here
    _          = _load_latest_csvs("Principals", date_str)

    if filings_df.empty or agents_df.empty:
        print("⚠️ Skipping enrichment: required files missing or unreadable.")
        return

    agents_df = agents_df.add_prefix("agent_")

    # Attempt common join keys
    possible_keys: List[Tuple[str, str]] = [
        ("entity_number", "agent_entity_number"),
        ("file_number",   "agent_file_number"),
        ("entity_id",     "agent_entity_id"),
    ]
    join_key = None
    for f_key, a_key in possible_keys:
        if f_key in filings_df.columns and a_key in agents_df.columns:
            join_key = (f_key, a_key)
            break
    if join_key is None:
        f_key, a_key = filings_df.columns[0], agents_df.columns[0]
        print(f"⚠️ Using fallback join keys: {f_key} ↔ {a_key}")
    else:
        f_key, a_key = join_key

    joined = filings_df.merge(
        agents_df,
        left_on=f_key,
        right_on=a_key,
        how="left",
        suffixes=("", "_agent"),
    )

    enriched = _apply_flags(joined)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    summary_counts = {}
    for tier in ["A", "B", "C"]:
        tier_df = enriched[enriched["lead_tier"] == tier].copy()
        out_file = OUTPUT_DIR / f"{tier}_leads_{date_str}.csv"
        if not tier_df.empty:
            tier_df.to_csv(out_file, index=False)
            summary_counts[tier] = len(tier_df)
        else:
            summary_counts[tier] = 0

    total = len(enriched)
    print("\n📊 Enrichment Summary:")
    print(f"Total leads processed: {total}")
    for tier in ["A", "B", "C"]:
        print(f" - Tier {tier}: {summary_counts[tier]}")
    print(f"Enriched files saved to: {OUTPUT_DIR.resolve()}")


if __name__ == "__main__":
    enrich_and_export()
