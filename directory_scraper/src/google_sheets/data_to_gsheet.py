"""Script to load or update data from `data/input` to Google Sheets.

- Each `org_id` has its own Google Sheet (spreadsheet, not a specific sheet).
- This script depends on the configuration file located at `/utils/json/spreadsheets_config.json` for managing and mapping organization IDs to their corresponding Google Sheet IDs.
- To ensure an organization is recognized as valid, update `spreadsheets_config.json` with the `org_id` and its corresponding Google Sheet ID.
"""

import json
import os
import argparse
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
from directory_scraper.src.google_sheets.process_data import validate_data, group_data_by_org_id, load_data_into_sheet, update_data_in_sheet, get_sheet_id
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
        load_data_into_sheet(google_sheets_manager, org_data, add_timestamp)
    elif operation == "update":
        update_data_in_sheet(google_sheets_manager, org_data, add_timestamp)
    else:
        print("Wrong operation. Only 'load' or 'update' is allowed.")

def process_all_orgs(data_folder, operation="update", add_timestamp=True):
    """Process all organizations listed in `spreadsheets_config.json`"""
    spreadsheets_config = load_spreadsheets_config()

    for entry in spreadsheets_config:
        org_id = entry['org_id']
        data_file = entry['data_file']

        try:
            sheet_id = get_sheet_id(org_id)
            org_data = load_org_data(data_folder, data_file)
            if org_data:
                print(f"\n 🟡 Processing org_id: {org_id}")
                main_process_org(data_folder, org_id, sheet_id, operation, add_timestamp, org_data)
            else:
                print(f"Skipping org_id: {org_id} as no matching data found in spider output.")
        except ValueError as e:
            print(f"Error processing org_id {org_id}: {e}")

def process_specific_org(data_folder, org_id, operation="update", add_timestamp=True):
    """Process a specific organization based on org_id."""
    spreadsheets_config = load_spreadsheets_config()
    valid_org_ids = {entry['org_id']: entry['data_file'] for entry in spreadsheets_config}

    if org_id not in valid_org_ids:
        print(f"org_id: {org_id} not found in the config. Skipping.")
        return

    try:
        sheet_id = get_sheet_id(org_id)
        data_file = valid_org_ids[org_id]
        org_data = load_org_data(data_folder, data_file)

        if not org_data:
            print(f"No matching data found for org_id: {org_id} in spider output.")
            return

        print(f"\n 🟡 Processing org_id: {org_id}")
        main_process_org(data_folder, org_id, sheet_id, operation, add_timestamp, org_data)
    except ValueError as e:
        print(f"Error processing org_id {org_id}: {e}")

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
        process_specific_org(org_id, operation, add_timestamp)
    else:
        process_all_orgs(operation, add_timestamp)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Performs either 'load' (clears & loads data) or 'update' data in Google Sheets. e.g: python data_to_sheet.py load")
    parser.add_argument("operation", choices=["load", "update"], help="Operation to perform: 'load' or 'update'.")
    parser.add_argument("--org_id", help="The organization ID (org_id) to limit processing to a specific organization.")

    args = parser.parse_args()
    main(operation=args.operation, org_id=args.org_id, add_timestamp=True)
