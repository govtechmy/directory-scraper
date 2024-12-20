"""
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
from directory_scraper.path_config import DEFAULT_SPIDERS_OUTPUT_FOLDER, DEFAULT_CLEAN_DATA_FOLDER
from directory_scraper.src.data_processing.process_data import process_all_json_files
from directory_scraper.src.data_processing.run_spiders import main as run_spiders_main
# from directory_scraper.src.elasticsearch_upload.data_to_es import main as data_to_es_main
from directory_scraper.src.google_sheets.data_to_gsheet import  process_specific_org
from directory_scraper.src.utils.file_utils import load_spreadsheets_config

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CLEAN_DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)
RAW_OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_SPIDERS_OUTPUT_FOLDER)
BACKUP_FOLDER = os.path.join(BASE_DIR, "backups")  

def main():
    
    # Step 1: Ensure directories exist
    os.makedirs(RAW_OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CLEAN_DATA_FOLDER, exist_ok=True)
    os.makedirs(BACKUP_FOLDER, exist_ok=True)

    # Step 2: Run spiders
    if len(sys.argv) < 2:
        print("Usage: python main.py <spider name | category | special keyword> [organization name] [subcategory]")
        return
    
    spiders_successful = False
    try:
        # Run spiders and proceed based on the presence of output files
        run_spiders_main(output_folder=RAW_OUTPUT_FOLDER, backup_folder=BACKUP_FOLDER)

        # Check if any files were generated in the output folder
        if not os.listdir(RAW_OUTPUT_FOLDER):
            print("No spiders were run or no data was collected. Skipping data processing and upload.")
            return
        elif os.listdir(RAW_OUTPUT_FOLDER):
            spiders_successful = True
    except Exception as e:
        print("Error encountered while running spiders:", e)
        return

    # Step 3: Process spiders output into clean data
    if spiders_successful:
        data_processing_successful = False
        try:
            process_all_json_files(input_folder=RAW_OUTPUT_FOLDER, output_folder=CLEAN_DATA_FOLDER)
            if not os.listdir(CLEAN_DATA_FOLDER):
                print("No data was cleaned nor processed. Skipping load data to Google Sheets")
                return
            elif os.listdir(CLEAN_DATA_FOLDER):
                print("Data processed and cleaned.")
                data_processing_successful = True
        except Exception as e:
            print("Data processing failed:", e)
            return

    # Step 4: Upload data to Google Sheets
    if data_processing_successful:
        print("\n============= Processing data upload to Google Sheets... =============")
        try:
            spreadsheets_config = load_spreadsheets_config()
            clean_data_files = os.listdir(CLEAN_DATA_FOLDER)
            
            for org_entry in spreadsheets_config:
                org_id = org_entry['org_id']
                data_file = org_entry['data_file']

                # Check if the matching data_file exists in CLEAN_DATA_FOLDER
                if data_file in clean_data_files:
                    print(f"✅ {org_id} - Found {data_file}. Uploading...")
                else:
                    print(f"❌ {org_id} - No matching data file found. Skipping.")

            for org_entry in spreadsheets_config:
                org_id = org_entry['org_id']
                data_file = org_entry['data_file']        
                
                if data_file in clean_data_files:
                    process_specific_org(CLEAN_DATA_FOLDER, org_id=org_id, operation="load", add_timestamp=True)

        except Exception as e:
            print("Error uploading data to Google Sheets:", e)
            return
        
        print(f"\nFinished workflow.")

if __name__ == "__main__":
    main()