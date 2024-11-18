import os
import json
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
from dotenv import load_dotenv
from utils.file_utils import load_spreadsheets_config
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_GSHEETS_OUTPUT_FOLDER, DEFAULT_LOG_DIR, DEFAULT_BACKUP_FOLDER

load_dotenv()

# Google Sheets API credentials
CREDS_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDS")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
OUTPUT_FOLDER = os.path.join(BASE_DIR,DEFAULT_GSHEETS_OUTPUT_FOLDER)
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


def main(output_folder=None, backup_folder=None):
    """
    Main function to fetch data from Google Sheets and store it in the specified folder.
    If no parameters are passed, the global OUTPUT_FOLDER and BACKUP_FOLDER will be used.
    """
    global OUTPUT_FOLDER, BACKUP_FOLDER
    OUTPUT_FOLDER = output_folder or OUTPUT_FOLDER
    BACKUP_FOLDER = backup_folder or BACKUP_FOLDER

    print(f"Using output folder: {OUTPUT_FOLDER}")
    print(f"Using backup folder: {BACKUP_FOLDER}")

    # Load spreadsheet configuration
    spreadsheets_config = load_spreadsheets_config()

    # Fetch and store data for each sheet in the configuration
    for sheet in spreadsheets_config:
        print(f"Fetching {sheet['org_id']}...")
        fetch_and_store_gsheet(sheet_id=sheet["sheet_id"], file_name=sheet["data_file"], output_folder=OUTPUT_FOLDER)

if __name__ == "__main__":
    main()
