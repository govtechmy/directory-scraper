"""Script to load or update data from `data/input` to Google Sheets.

- Each `org_id` has its own Google Sheet (spreadsheet, not a specific sheet).
- This script depends on the configuration file located at `/utils/json/spreadsheets_config.json` for managing and mapping organization IDs to their corresponding Google Sheet IDs.
- To ensure an organization is recognized as valid, update `spreadsheets_config.json` with the `org_id` and its corresponding Google Sheet ID.
"""

import json
import os
import argparse
from google_sheets_utils import GoogleSheetManager
from process_data import validate_data, group_data_by_org_id, load_data_into_sheet, update_data_in_sheet
from utils.file_utils import load_spreadsheets_config
from dotenv import load_dotenv

load_dotenv()
script_dir = os.path.dirname(os.path.abspath(__file__))

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DATA_DIRECTORY = os.path.join(script_dir, '../data_processing/data/output/')

def load_org_data(data_file):
    """
    Load data from the specified JSON file in DATA_DIRECTORY.
    """
    file_path = os.path.join(DATA_DIRECTORY, data_file)
    if not os.path.exists(file_path):
        print(f"Data file {data_file} not found in {DATA_DIRECTORY}. Skipping.")
        return None

    with open(file_path, 'r') as f:
        data = json.load(f)
    
    return validate_data(data)

def main_process_org(org_id, sheet_id, operation, add_timestamp, org_data):
    google_sheets_manager = GoogleSheetManager(GOOGLE_SERVICE_ACCOUNT_CREDS, sheet_id, SCOPES)
    if operation == "load":
        google_sheets_manager.clear_sheet()
        load_data_into_sheet(google_sheets_manager, org_data, add_timestamp)
    elif operation == "update":
        update_data_in_sheet(google_sheets_manager, org_data, add_timestamp)
    else:
        print("Wrong operation. Only 'load' or 'update' is allowed.")
    print(f"Finished processing org_id: {org_id}")

def process_all_orgs(operation="update", add_timestamp=True):
    """Process all organizations in the spreadsheets_config.json."""
    spreadsheets_config = load_spreadsheets_config()
    valid_org_ids = {org['org_id']: (org['sheet_id'], org['data_file']) for org in spreadsheets_config}

    for org_id, (sheet_id, data_file) in valid_org_ids.items():
        org_data = load_org_data(data_file)
        if org_data:
            print(f"Processing org_id: {org_id} with sheet_id: {sheet_id}")
            main_process_org(org_id, sheet_id, operation, add_timestamp, org_data)
        else:
            print(f"Skipping org_id: {org_id} as no matching data found in spider output.")


def process_specific_org(org_id, operation="update", add_timestamp=True):
    """Process a specific organization based on org_id."""
    spreadsheets_config = load_spreadsheets_config()
    valid_org_ids = {org['org_id']: (org['sheet_id'], org['data_file']) for org in spreadsheets_config}

    if org_id not in valid_org_ids:
        print(f"org_id: {org_id} not found in the config. Skipping.")
        print(f"Available org_id: {list(valid_org_ids.keys())}. Else, update the config file.")
        return

    sheet_id, data_file = valid_org_ids[org_id]
    org_data = load_org_data(data_file)  # Use data_file instead of org_id
    if not org_data:
        print(f"No matching data found for org_id: {org_id} in spider output.")
        return

    print(f"Processing org_id: {org_id} with sheet_id: {sheet_id}")
    main_process_org(org_id, sheet_id, operation, add_timestamp, org_data)


def main(operation="update", org_id=None, add_timestamp=True):
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
