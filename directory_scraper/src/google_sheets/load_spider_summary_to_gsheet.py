import json
import os
from datetime import datetime
from dotenv import load_dotenv
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_LOG_DIR
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
from directory_scraper.src.google_sheets.process_data import retry_with_backoff
from directory_scraper.src.google_sheets.data_to_gsheet import get_gsheet_id

load_dotenv()
CREDS_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDS")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

LOG_DIR = os.path.join(DEFAULT_LOG_DIR)
SPIDER_SUMMARY_FILE = os.path.join(LOG_DIR, 'spider_summary.json')

def load_spider_summary(ref_name, sheet_id, data, add_timestamp=True):
    if not data:
        print("No data to load into Google Sheets.")
        return

    row_count_summary = {}

    retry_with_backoff(google_sheets_manager.switch_to_sheet, "Summary Logs")

    header = list(data[0].keys())
    if add_timestamp:
        header.append('last_uploaded')
        
    existing_data = google_sheets_manager.get_all_data()
    if not existing_data or existing_data[0] != header:
        retry_with_backoff(google_sheets_manager.append_rows, [header])

    rows_to_upload = []
    for spider_entry in data:
        row = list(spider_entry.values())
        if add_timestamp:
            row.append(datetime.now().isoformat())
        rows_to_upload.append(row)

    retry_with_backoff(google_sheets_manager.append_rows, rows_to_upload)

if __name__ == "__main__":

    if not os.path.exists(DEFAULT_LOG_DIR):
        os.makedirs(DEFAULT_LOG_DIR)

    if not os.path.exists(SPIDER_SUMMARY_FILE):
        print(f"Error: JSON file '{SPIDER_SUMMARY_FILE}' not found.")
        exit(1)

    with open(SPIDER_SUMMARY_FILE, "r") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON file.")
            exit(1)

    ref_name = "spider_summary"
    sheet_id = get_gsheet_id(ref_name)
    google_sheets_manager = GoogleSheetManager(CREDS_FILE, sheet_id, SCOPES)

    load_spider_summary(ref_name, google_sheets_manager, data)
