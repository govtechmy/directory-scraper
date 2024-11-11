"""Script to load or update data from `data/input` to Google Sheets.

- Each `org_id` has its own Google Sheet (spreadsheet, not a specific sheet).
- This script depends on the configuration file located at `/utils/json/spreadsheets_config.json` for managing and mapping organization IDs to their corresponding Google Sheet IDs.
- To ensure an organization is recognized as valid, update `spreadsheets_config.json` with the `org_id` and its corresponding Google Sheet ID.
"""

import json
import os
import argparse
from google_sheets_utils import GoogleSheetManager
from process_data import validate_data, group_data_by_org_id
from process_data import load_data_into_sheet
from process_data import update_data_in_sheet
from utils.file_utils import load_spreadsheets_config
from dotenv import load_dotenv

load_dotenv()

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
JSON_DATA_PATH = 'data/data.json'

def main_process_org(org_id, sheet_id, operation, add_timestamp, org_data):
    """
    Process the data for a single org_id and synchronize with Google Sheets.
    Args:
        org_id: The organization ID to process.
        sheet_id: The Google Sheet ID corresponding to the organization.
        operation: Either "load" or "update".
        add_timestamp: Whether to add a timestamp to the data.
        org_data: The data specific to the org_id.
    """
    # Connect to Google Sheet using the sheet_id
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
    """
    Process all organizations in the spreadsheets_config.json.
    """
    spreadsheets_config = load_spreadsheets_config()
    # Extract valid org_ids from spreadsheets_config.json
    valid_org_ids = {org['org_id']: org['sheet_id'] for org in spreadsheets_config}
    data = validate_data(JSON_DATA_PATH)
    grouped_data = group_data_by_org_id(data)
    for org_id, sheet_id in valid_org_ids.items():
        if org_id in grouped_data:
            print(f"Processing org_id: {org_id} with sheet_id: {sheet_id}")
            main_process_org(org_id, sheet_id, operation, add_timestamp, grouped_data[org_id])
        else:
            print(f"Skipping org_id: {org_id} as no matching data found in JSON.")

def process_specific_org(org_id, operation="update", add_timestamp=True):
    """
    Process a specific organization based on org_id.
    """
    spreadsheets_config = load_spreadsheets_config()
    # Extract valid org_ids from spreadsheets_config.json
    valid_org_ids = {org['org_id']: org['sheet_id'] for org in spreadsheets_config}
    data = validate_data(JSON_DATA_PATH)
    grouped_data = group_data_by_org_id(data)

    # Handle the specific org_id
    if org_id not in valid_org_ids:
        print(f"org_id: {org_id} not found in the config. Skipping.")
        print(f"Available org_id: {list(valid_org_ids.keys())}. Else, update the config file.")
        return

    if org_id not in grouped_data:
        print(f"No matching data found for org_id: {org_id} in the JSON data.")
        print("Check if the org_id is available")

        return

    # Process the specified org_id
    sheet_id = valid_org_ids[org_id]
    print(f"Processing org_id: {org_id} with sheet_id: {sheet_id}")
    main_process_org(org_id, sheet_id, operation, add_timestamp, grouped_data[org_id])


def main(operation="update", org_id=None, add_timestamp=True):
    """
    Main function to load or update data for either a specific org or all orgs.
    Args:
        org_id: Organization ID to limit processing to a specific organization (if provided).
        operation: Either "load" or "update" (default is "load").
        add_timestamp: Whether to add a timestamp to the data (default is True).
    """
    if org_id:
        # Process a specific organization
        process_specific_org(org_id, operation, add_timestamp)
    else:
        # Process all organizations
        process_all_orgs(operation, add_timestamp)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Performs either 'load' (clears & loads data) or 'update' data in Google Sheets. e.g: python data_to_sheet.py load")
    parser.add_argument("operation", choices=["load", "update"], help="Operation to perform: 'load' or 'update'.")
    parser.add_argument("--org_id", help="The organization ID (org_id) to limit processing to a specific organization.")

    args = parser.parse_args()

    main(operation=args.operation, org_id=args.org_id, add_timestamp=True)
