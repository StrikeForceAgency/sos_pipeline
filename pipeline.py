# pipeline.py

from scripts.extract_and_rename import extract_zip_and_rename

def main():
    print("Running SOS Pipeline...")
    extract_zip_and_rename()
    # Add future steps here (cleaning, merging, enrichment, etc.)
    print("Pipeline completed.")

if __name__ == "__main__":
    main()
