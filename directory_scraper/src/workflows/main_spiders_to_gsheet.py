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
from directory_scraper.src.utils.discord_bot import send_discord_notification

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
CLEAN_DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)
RAW_OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_SPIDERS_OUTPUT_FOLDER)
BACKUP_FOLDER = os.path.join(BASE_DIR, "backups")
DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL') 
THREAD_ID = os.getenv('THREAD_ID')

row_summary = {}

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
        if DISCORD_WEBHOOK_URL:
            send_discord_notification(f"\n❗❗❗ Error encountered while running spiders: {e}", DISCORD_WEBHOOK_URL, THREAD_ID)
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
            # Load configuration and data files
            spreadsheets_config = load_spreadsheets_config()
            clean_data_files = os.listdir(CLEAN_DATA_FOLDER)
            processed_items = set()
            to_process = []  # Collect valid entries for processing

            # Step 1: Generate the full list of entries
            for org_entry in spreadsheets_config:
                org_id = org_entry['org_id']
                data_file = org_entry['data_file']
                ref_name = org_entry['ref_name']

                # Check if the data file exists
                if data_file in clean_data_files:
                    if (ref_name, data_file, org_id) in processed_items:
                        print(f"Skipping duplicate: {ref_name} (org_id: {org_id}, file: {data_file})")
                        continue
                    print(f"✅ {org_id} - Found {data_file}. Uploading...")
                    to_process.append((ref_name, data_file, org_id))  # Add to process later
                else:
                    print(f"☑️ {org_id} - No matching data file found. Skipping.")

            # Step 2: Process valid entries
            for ref_name, data_file, org_id in to_process:
                processed_items.add((ref_name, data_file, org_id))
                rows_processed = process_specific_org(CLEAN_DATA_FOLDER, ref_name, data_file, org_id, operation="load", add_timestamp=True)
                row_summary[(ref_name, org_id)] = rows_processed

        except Exception as e:
            print("Error uploading data to Google Sheets:", e)
            if DISCORD_WEBHOOK_URL:
                send_discord_notification(f"\n❗❗❗ Error uploading data to Google Sheets: {e}", DISCORD_WEBHOOK_URL, THREAD_ID)
            return

        print("\n📗 GOOGLE SHEETS SUMMARY (no.of rows inserted):")
        summary_messages = []
        for (ref_name, org_id), rows in row_summary.items():
            message = f"- {ref_name}: {rows} rows"
            print(message)
            summary_messages.append(message)
        final_summary_message = "\n".join(summary_messages)
        if DISCORD_WEBHOOK_URL:
            send_discord_notification(f"\n📗 GOOGLE SHEETS SUMMARY (no.of rows inserted)\n{final_summary_message}", DISCORD_WEBHOOK_URL, THREAD_ID)

    print("\nFinished workflow.")

if __name__ == "__main__":
    main()