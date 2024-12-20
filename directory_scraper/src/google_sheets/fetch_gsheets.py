import os
import json
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
from dotenv import load_dotenv
from directory_scraper.src.utils.file_utils import load_spreadsheets_config
from directory_scraper.src.google_sheets.process_data import get_sheet_id
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_GSHEETS_OUTPUT_FOLDER, DEFAULT_LOG_DIR, DEFAULT_BACKUP_FOLDER

load_dotenv()

# Google Sheets API credentials
CREDS_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDS")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
OUTPUT_FOLDER = os.path.join(BASE_DIR, DEFAULT_GSHEETS_OUTPUT_FOLDER)
BACKUP_FOLDER = os.path.join(BASE_DIR, DEFAULT_BACKUP_FOLDER)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(BACKUP_FOLDER, exist_ok=True)


def fetch_and_store_gsheet(sheet_id, file_name, output_folder):
    """
    Fetch data from Google Sheets and store it as a JSON file.
    """
    try:
        # Initialize Google Sheet Manager
        sheet_manager = GoogleSheetManager(CREDS_FILE, sheet_id, SCOPES)

        # Fetch all data
        data = sheet_manager.get_all_data()

        # Process column names and data rows
        column_names, data_without_header = data[0], data[1:]
        data_as_dict = [
            {column_names[i]: row[i] for i in range(len(column_names))}
            for row in data_without_header
        ]

        # Save data to JSON file
        output_file_path = os.path.join(output_folder, file_name)
        with open(output_file_path, "w") as json_file:
            json.dump(data_as_dict, json_file, indent=4)

        print(f"Successfully fetched and stored data: {output_file_path}")
    except Exception as e:
        print(f"Error fetching or storing data for file_name={file_name}: {e}")


def main(org_id=None, output_folder=None, backup_folder=None):
    """
    Main function to fetch data for a specific org_id from Google Sheets.
    If no parameters are passed, the global OUTPUT_FOLDER and BACKUP_FOLDER will be used.
    """
    global OUTPUT_FOLDER, BACKUP_FOLDER
    OUTPUT_FOLDER = output_folder or OUTPUT_FOLDER
    BACKUP_FOLDER = backup_folder or BACKUP_FOLDER

    print(f"Using output folder: {OUTPUT_FOLDER}")
    print(f"Using backup folder: {BACKUP_FOLDER}")

    # Load spreadsheet configuration
    spreadsheets_config = load_spreadsheets_config()

    # Option 1: if org_id is specified in parameter, filter for that specific org_id only (default org_id=None)
    if org_id:
        try:
            sheet_id = get_sheet_id(org_id)
            # Filter spreadsheets_config to only include the specified org_id
            spreadsheets_config = [sheet for sheet in spreadsheets_config if sheet["org_id"] == org_id]
            if not spreadsheets_config:
                print(f"No configuration found for org_id={org_id}.")
                return
            # Process the single org_id
            sheet = spreadsheets_config[0]
            print(f"Fetching {org_id}...")
            fetch_and_store_gsheet(sheet_id=sheet_id, file_name=sheet["data_file"], output_folder=OUTPUT_FOLDER)

        except ValueError as e:
            print(f"Error: {e}")
            return
        
    # Option 2: if no org_id is specified, process all org_id in spreadsheets_config.json
    else:
        for sheet in spreadsheets_config:
            try:
                sheet_id = get_sheet_id(sheet["org_id"])
                print(f"Fetching {sheet['org_id']}...")
                fetch_and_store_gsheet(sheet_id=sheet_id, file_name=sheet["data_file"], output_folder=OUTPUT_FOLDER)
            except ValueError as e:
                print(f"Error processing org_id {sheet['org_id']}: {e}")

if __name__ == "__main__":
    # Example usage: specify an org_id or leave it as None to fetch all sheets.
    # e.g python main_gsheet_to_es.py JPM
    org_id = sys.argv[1] if len(sys.argv) > 1 else None
    main(org_id=org_id)
