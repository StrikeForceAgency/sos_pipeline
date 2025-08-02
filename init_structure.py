from pathlib import Path

# Use your local project root
root = Path.cwd()  # Automatically uses the folder where you're running this script

folders = [
    "raw", "raw_zips", "outputs", "archive", 
    "logs", "logs/errors", "logs/success", "logs/debug",
    "enrichments", "scripts"
]

# Create folders
for folder in folders:
    (root / folder).mkdir(parents=True, exist_ok=True)

# README content
readme_content = {
    "raw": "# Raw CSVs\nThis folder stores the original unzipped CSV files from California SOS.\nUsually contains Filings.csv, Agents.csv, and Principals.csv before parsing.",
    "raw_zips": "# Raw Zips\nThis is where you drop the original .zip files downloaded from SOS.\nThey remain untouched for archival and reprocessing if needed.",
    "outputs": "# Outputs\nThis folder holds cleaned, merged, and enriched output files.\nEach output should be timestamped and named based on processing logic.",
    "archive": "# Archive\nOlder input files, historical output snapshots, and archived datasets go here.\nThis helps keep a clean workspace while preserving past runs.",
    "enrichments": "# Enrichments\nStores enriched data like email, phone, LinkedIn, and website results\nfrom SerpAPI, Bing, or other enrichment tools.",
    "logs": "# Logs\nContains log files for every run. Includes:\n- errors\n- debug steps\n- successful runs\nUsed to troubleshoot failures or data mismatches.",
    "scripts": "# Scripts\nHelper scripts and utilities for parsing, filtering, deduping, or API lookups.\nNon-pipeline helpers can be kept here for modular support."
}

# Write readmes
for folder, content in readme_content.items():
    readme_path = root / folder / "README.md"
    readme_path.write_text(content)

print("✅ Project structure and README files created successfully.")
