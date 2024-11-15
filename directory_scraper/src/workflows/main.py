import sys
import os
import subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_SPIDERS_OUTPUT_FOLDER, DEFAULT_CLEAN_DATA_FOLDER
from directory_scraper.src.data_processing.process_data import process_all_json_files
from directory_scraper.src.elasticsearch.data_to_es import upload_clean_data_to_es, check_sha_and_update
from directory_scraper.src.data_processing.run_spiders import main as run_spiders_main
from directory_scraper.src.elasticsearch.data_to_es import main as data_to_es_main

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CLEAN_DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)
SPIDERS_OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_SPIDERS_OUTPUT_FOLDER)
BACKUP_FOLDER = os.path.join(BASE_DIR, "backups")  


def main():
    # Step 1: Ensure directories exist
    os.makedirs(SPIDERS_OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CLEAN_DATA_FOLDER, exist_ok=True)
    os.makedirs(BACKUP_FOLDER, exist_ok=True)

    # Step 2: Run spiders
    if len(sys.argv) < 2:
        print("Usage: python main.py <spider name | category | special keyword> [organization name] [subcategory]")
        return
    
    try:
        # Run spiders and proceed based on the presence of output files
        run_spiders_main(output_folder=SPIDERS_OUTPUT_FOLDER, backup_folder=BACKUP_FOLDER)

        # Check if any files were generated in the output folder
        if not os.listdir(SPIDERS_OUTPUT_FOLDER):
            print("No spiders were run or no data was collected. Skipping data processing and upload.")
            return
    except Exception as e:
        print("Error encountered while running spiders:", e)
        return

    # Step 3: Process spiders output into clean data
    try:
        print("\nSpiders ran successfully. Proceeding with data processing...")
        process_all_json_files(input_folder=SPIDERS_OUTPUT_FOLDER, output_folder=CLEAN_DATA_FOLDER)
    except Exception as e:
        print("Data processing failed:", e)
        return
    
    # Step 4: Check SHA and upload to Elasticsearch if there are changes
    try:
        print("\nChecking for changes and uploading to Elasticsearch...")
        data_to_es_main(CLEAN_DATA_FOLDER)
    except Exception as e:
        print("Elasticsearch upload failed:", e)

if __name__ == "__main__":
    main()