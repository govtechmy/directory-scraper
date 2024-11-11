"""Main app for loading and updating data in Google Sheet"""

from google_sheets_utils import GoogleSheetManager
from process_data import validate_data, load_data_into_sheet, update_data_in_sheet

import os
from dotenv import load_dotenv
import argparse

load_dotenv()

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
JSON_DATA_PATH = 'data/data.json'

def main(operation="load", add_timestamp=True):
    """
    Main function to load data, process it, and insert/update it into Google Sheets.
    Args:
        operation: Either "load" or "update" (default is "load").
        add_timestamp: Whether to add a timestamp to the data (default is True).

    If "load", the sheet will be cleared completely, and then new data will be loaded.
    If "update", only relevant data will be deleted (is identified by org_id), and then new data will be inserted (appended).
    """
    # Connect to Google Sheet
    google_sheets_manager = GoogleSheetManager(GOOGLE_SERVICE_ACCOUNT_CREDS, GOOGLE_SHEET_ID, SCOPES)
    worksheet = google_sheets_manager.worksheet
    
    # Validate and load data
    data = validate_data(JSON_DATA_PATH)
    
    if operation == "load":
        # Clear the sheet (optional for load)
        google_sheets_manager.clear_sheet()
        # Process and insert the data
        load_data_into_sheet(google_sheets_manager, data, add_timestamp)
    elif operation == "update":
        # Update existing data in the sheet based on org_id
        update_data_in_sheet(google_sheets_manager, data, add_timestamp)

if __name__ == '__main__':
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Performs either 'load'(clears & load data) or 'update' data in Google Sheets. e.g: python main.py load")
    
    # Add command-line arguments
    parser.add_argument("operation", choices=["load", "update"], help="Operation to perform: 'load' or 'update'.")

    # Parse arguments from the command line
    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(operation=args.operation, add_timestamp=True)