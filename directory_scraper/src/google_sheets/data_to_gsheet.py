"""Script to load or update data from `data/input` to Google Sheets.

- Each `org_id` has its own Google Sheet (spreadsheet, not a specific sheet).
- This script depends on the configuration file located at `/utils/json/gsheets_config.json` for managing and mapping organization IDs to their corresponding Google Sheet IDs.
- To ensure an organization is recognized as valid, update `gsheets_config.json` with the `org_id` and its corresponding Google Sheet ID.
"""

import json
import os
import argparse
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
from directory_scraper.src.google_sheets.process_data import validate_data, group_data_by_org_id, load_data_into_sheet, update_data_in_sheet, get_gsheet_id
from directory_scraper.src.utils.file_utils import load_spreadsheets_config
from directory_scraper.path_config import DEFAULT_CLEAN_DATA_FOLDER
from dotenv import load_dotenv

load_dotenv()
script_dir = os.path.dirname(os.path.abspath(__file__))

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)

def load_org_data(data_folder, data_file):
    """
    Load data from the specified JSON file in DATA_FOLDER.
    """
    file_path = os.path.join(data_folder, data_file)
    if not os.path.exists(file_path):
        print(f"Data file {data_file} not found in {data_folder}. Skipping.")
        return None

    with open(file_path, 'r') as f:
        data = json.load(f)
    
    return validate_data(data)

def main_process_org(data_folder, org_id, sheet_id, operation, add_timestamp, org_data):
    google_sheets_manager = GoogleSheetManager(GOOGLE_SERVICE_ACCOUNT_CREDS, sheet_id, SCOPES)
    if operation == "load":
        google_sheets_manager.clear_sheet()
        row_summary = load_data_into_sheet(google_sheets_manager, org_data, add_timestamp=True, separate_grouped_data_by_sheet=True, group_key="division_name")
    elif operation == "update":
        row_summary = update_data_in_sheet(google_sheets_manager, org_data, add_timestamp)
    else:
        print("Wrong operation. Only 'load' or 'update' is allowed.")
        return 0
    
    return sum(row_summary.values())

def process_all_orgs(data_folder, operation="update", add_timestamp=True):
    """Process all organizations listed in `gsheets_config.json`"""
    spreadsheets_config = load_spreadsheets_config()
    processed_items = set()  # Track already processed (ref_name, data_file, org_id)

    for entry in spreadsheets_config:
        ref_name = entry['ref_name']
        data_file = entry['data_file']
        org_id = entry['org_id']

        # Skip duplicates
        if (ref_name, data_file, org_id) in processed_items:
            print(f"Skipping already processed: {ref_name} (org_id: {org_id}, file: {data_file})")
            continue

        try:
            # Call process_specific_org directly for each entry
            process_specific_org(
                data_folder=data_folder,
                ref_name=ref_name,
                data_file=data_file,
                org_id=org_id,
                operation=operation,
                add_timestamp=add_timestamp
            )
            processed_items.add((ref_name, data_file, org_id))  # Mark as processed
        except Exception as e:
            print(f"Error processing ref_name {ref_name} (data_file: {data_file}): {e}")

def process_specific_org(data_folder, ref_name, data_file, org_id, operation="update", add_timestamp=True):
    """Process a specific ref_name and data_file for the given org_id."""
    try:
        sheet_id = get_gsheet_id(ref_name)
        org_data = load_org_data(data_folder, data_file)

        if not org_data:
            print(f"No data found for {ref_name} (org_id: {org_id} - data_file: {data_file})")
            return 0

        print(f"\n 🟡 Processing: {ref_name} (org_id: {org_id})")
        rows_processed = main_process_org(data_folder, org_id, sheet_id, operation, add_timestamp, org_data)
        # print(f"✅ {org_id}: {rows_processed} rows inserted.")
        return rows_processed
    
    except ValueError as e:
        print(f"Error processing org_id {org_id}: {e}")
        return 0 

def main(data_folder=None, operation="update", org_id=None, add_timestamp=True):
    data_folder = data_folder or DATA_FOLDER
    """
    Main function to load or update data for either a specific org or all orgs.
    Args:
        org_id: Organization ID to limit processing to a specific organization (if provided).
        operation: Either "load" or "update" (default is "load").
        add_timestamp: Whether to add a timestamp to the data (default is True).
    """
    if org_id:
        spreadsheets_config = load_spreadsheets_config()
        # Find the matching configuration for the given org_id
        entry = [config for config in spreadsheets_config if config['org_id'] == org_id]
        if not entry:
            print(f"Error: org_id '{org_id}' not found in configuration.")
            return
        entry = entry[0]
        process_specific_org(data_folder=data_folder, ref_name=entry['ref_name'], data_file=entry['data_file'], org_id=org_id, operation=operation, add_timestamp=add_timestamp)
    else:
        process_all_orgs(data_folder=data_folder, operation=operation, add_timestamp=add_timestamp)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Performs either 'load' (clears & loads data) or 'update' data in Google Sheets. e.g: python data_to_sheet.py load")
    parser.add_argument("operation", choices=["load", "update"], help="Operation to perform: 'load' or 'update'.")
    parser.add_argument("--org_id", help="The organization ID (org_id) to limit processing to a specific organization.")

    args = parser.parse_args()
    main(operation=args.operation, org_id=args.org_id, add_timestamp=True)
