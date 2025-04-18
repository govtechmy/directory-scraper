"""
SOURCE OF TRUTH: Google Sheets

IMPORTANT: 
1. The uploaded data is considered the source of truth.
   - Any new data provided will completely overwrite the old data for the same `org_id`.
   - It is expected that the new data is complete and not a snippet or subset of the previous data.

2. Incomplete or partial data may result in loss of information, as the old data not present in the new upload
   will be treated as stale and deleted.

3. Ensure that the new data represents the latest information before uploading.
"""
import sys
import os
import subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_GSHEETS_OUTPUT_FOLDER, DEFAULT_CLEAN_DATA_FOLDER
from directory_scraper.src.data_processing.process_data import process_all_json_files
from directory_scraper.src.google_sheets.fetch_gsheets import main as fetch_gsheet_main
from directory_scraper.src.elasticsearch_upload.data_to_es import main as data_to_es_main

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CLEAN_DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)
RAW_OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_GSHEETS_OUTPUT_FOLDER)

def main():
    print("IMPORTANT: The newer data will overwrite existing data for the same 'org_id' in Elasticsearch.")
    print("Ensure the uploaded file is complete and represents the latest information.")
    
    # Step 1: Ensure directories exist
    os.makedirs(RAW_OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CLEAN_DATA_FOLDER, exist_ok=True)
    # os.makedirs(BACKUP_FOLDER, exist_ok=True)

    ref_name = sys.argv[1] if len(sys.argv) > 1 else None
    if len(sys.argv) > 2:
        print("Usage: python main_gsheets.py [ref_name]. Refer to: utils/json/gsheets_config.json for ref_name")
        return

    # Step 1: Fetch Google Sheets
    try:
        # Run fetch and proceed based on the presence of output files
        fetch_gsheet_main(sheet_name="Keseluruhan Direktori", ref_name=ref_name, output_folder=RAW_OUTPUT_FOLDER, backup_folder=None)

        # Check if any files were generated in the output folder
        if not os.listdir(RAW_OUTPUT_FOLDER):
            print("No google sheets was fetched. Skipping data processing and upload.")
            return
    except Exception as e:
        print("Error encountered while fetching google sheets:", e)
        return

    # # Step 2 Process spiders output into clean data
    # try:
    #     print("\nProceeding with data processing...")
    #     process_all_json_files(input_folder=RAW_OUTPUT_FOLDER, output_folder=CLEAN_DATA_FOLDER)
    # except Exception as e:
    #     print("Data processing failed:", e)
    #     return
    
    # Step 3: Check SHA and upload to Elasticsearch if there are changes
    try:
        print("\nChecking for changes and uploading to Elasticsearch...")
        data_to_es_main(RAW_OUTPUT_FOLDER)
    except Exception as e:
        print("Elasticsearch upload failed:", e)

if __name__ == "__main__":
    main()