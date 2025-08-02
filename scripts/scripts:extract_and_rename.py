import zipfile
from pathlib import Path
import shutil
import datetime

RAW_ZIPS_DIR = Path("raw_zips")
RAW_DIR = Path("raw")

# Create today's date stamp
timestamp = datetime.datetime.now().strftime("%Y-%m-%d")

def extract_and_rename():
    for zip_file in RAW_ZIPS_DIR.glob("*.zip"):
        print(f"📦 Extracting: {zip_file.name}")

        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            extract_path = RAW_DIR / zip_file.stem
            extract_path.mkdir(parents=True, exist_ok=True)
            zip_ref.extractall(extract_path)

        # Look for CSVs inside the nested DataRequest folder
        nested_dirs = list(extract_path.glob("DataRequest*"))
        if not nested_dirs:
            print(f"⚠️ No nested 'DataRequest...' folder found in {zip_file.name}")
            continue

        for file in nested_dirs[0].glob("*.csv"):
            name_map = {
                "Filings.csv": f"Filings_{timestamp}.csv",
                "Agents.csv": f"Agents_{timestamp}.csv",
                "Principals.csv": f"Principals_{timestamp}.csv"
            }

            new_name = name_map.get(file.name)
            if new_name:
                shutil.move(str(file), str(RAW_DIR / new_name))
                print(f"✅ {file.name} → {new_name}")
            else:
                print(f"❌ Unknown file: {file.name}")

        # Optional: move the zip to archive
        archive_dir = Path("archive") / "raw_zips"
        archive_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(zip_file), archive_dir / zip_file.name)

        # Cleanup: remove the extracted folder
        shutil.rmtree(extract_path)

    print("🎉 Extraction and renaming complete.")

if __name__ == "__main__":
    extract_and_rename()
    
