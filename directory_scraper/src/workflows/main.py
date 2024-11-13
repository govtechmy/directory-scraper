import sys
import os
import subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_SPIDERS_OUTPUT_FOLDER, DEFAULT_CLEAN_DATA_FOLDER
from directory_scraper.src.data_processing.process_data import process_all_json_files
from directory_scraper.src.elasticsearch.data_to_es import upload_clean_data_to_es, check_sha_and_update
from directory_scraper.src.data_processing.run_spiders import main as run_spiders_main

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CLEAN_DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)
SPIDERS_OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_SPIDERS_OUTPUT_FOLDER)
BACKUP_FOLDER = os.path.join(BASE_DIR, "backups")  


def main():
    # Step 1: Ensure directories exist
    os.makedirs(SPIDERS_OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(CLEAN_DATA_FOLDER, exist_ok=True)
    os.makedirs(BACKUP_FOLDER, exist_ok=True)

    # Parse command-line arguments as required by run_spiders.py
    if len(sys.argv) < 2:
        print("Usage: python main.py <spider name | category | special keyword> [organization name] [subcategory]")
        return

    spider_name = sys.argv[1]  # Required argument
    org_name = sys.argv[2] if len(sys.argv) > 2 else None  # Optional organization name
    subcategory = sys.argv[3] if len(sys.argv) > 3 else None  # Optional subcategory

    print(f"Running spider(s) with: name='{spider_name}', org_name='{org_name}', subcategory='{subcategory}'")

    try:
        run_spiders_main(
            spider_list=[spider_name],
            output_folder=SPIDERS_OUTPUT_FOLDER,
            backup_folder=BACKUP_FOLDER
        )
    except Exception as e:
        print("Error encountered while running spiders:", e)
        print("Spiders did not run successfully. Skipping data processing and data upload.")
        return

    print("Spiders ran successfully. Proceeding with data processing...")
    process_all_json_files(input_folder=SPIDERS_OUTPUT_FOLDER, output_folder=CLEAN_DATA_FOLDER)
    print("Data processing completed and cleaned data stored in:", CLEAN_DATA_FOLDER)
    
    # Step 4: Check SHA and upload to Elasticsearch if there are changes
    print("Checking for changes in data compared to in Elasticsearch...")
    sha_changed = check_sha_and_update(CLEAN_DATA_FOLDER)
    
    if sha_changed:
        print("Data has changed. Uploading newer data to Elasticsearch...")
        upload_clean_data_to_es(CLEAN_DATA_FOLDER)
        print("Data upload to Elasticsearch completed.")
    else:
        print("No changes detected in data. Skipping Elasticsearch upload.")

if __name__ == "__main__":
    main()