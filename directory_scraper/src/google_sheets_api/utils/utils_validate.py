import os
import json
import logging
import hashlib
import datetime
import pandas as pd
from pathlib import Path
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from directory_scraper.src.google_sheets_api.utils.utils_gsheet import GoogleSheetManager
from directory_scraper.src.data_processing.process_data import data_processing_pipeline
from directory_scraper.src.elasticsearch_upload.data_to_es import calculate_sha256_for_file

# Setup
load_dotenv()

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

logger = logging.getLogger(__name__)
logging.basicConfig(filename="gsheet_api.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(pathname)s - Line %(lineno)d - %(levelname)s - %(message)s')


# Replaces process_json_data from data_processing.process_data
# Function now takes json object as the input, instead of a file path
def process_json_data(input_data):
    """Function to process a single JSON file and return the result"""
    try:
        if isinstance(input_data, list):
            processed_data = data_processing_pipeline(input_data)
            
            return processed_data
        else:
            logging.warning(f"Invalid JSON format in input_data, skipping file.")
            return
    except Exception as e:
        logging.error(f"An error occurred while processing input_data: {str(e)}")
        raise e


def clean_data(sheet_id:str, user_name:str) -> dict:
    """
    Loads latest data from google sheet and compares the SHA256 hexdigest of the current data with the previous hexdigest.
    If there are any changes, validate the data and push to ElasticSearch.
    """
    # Setup google sheet
    sheet_manager = GoogleSheetManager(GOOGLE_SERVICE_ACCOUNT_CREDS, sheet_id, SCOPES)
    sheet_data = sheet_manager.get_all_data(sheet_id=0)

    # Extract data and generate document hash
    sheet_df = pd.DataFrame(sheet_data)
    sheet_df = (
        sheet_df
        .rename(columns=sheet_df.iloc[0])
        .drop(index=0, columns="last_uploaded")
        .reset_index(drop=True)
        .astype({"org_sort": int, "division_sort": int, "position_sort": int})
    )
    document_hash = calculate_sha256_for_file(sheet_df.to_dict(orient="records"))

    # Extract latest hash from "Edit Logs" for comparison
    edit_logs = sheet_manager.connect_to_sheet().worksheet("Edit Logs")
    last_row = max(0, edit_logs.acell("D2").numeric_value)
    previous_hash = edit_logs.acell(f"B{1+last_row}").value

    if document_hash != previous_hash:
        cleaned_data = process_json_data(sheet_df.to_dict(orient="records"))
        
        # Append new hash to "Edit Logs" worksheet
        current_time = datetime.datetime.now(ZoneInfo("Asia/Kuala_Lumpur")).strftime("%Y-%m-%d %H:%M:%S")
        edit_logs.append_row([user_name, document_hash, current_time], table_range=f"A{2+last_row}")
        logging.info("Changes detected, updating 'Edit Logs' sheet.")

    else:
        cleaned_data = sheet_df.to_dict(orient="records")
        logging.info("No changes made.")

    return {"cleaned_data": cleaned_data, "document_hash": document_hash, "previous_hash": previous_hash, "current_hash": document_hash}

if __name__ == "__main__":
    ROOT_DIR = Path(os.path.abspath(__file__)).parents[2]
    input_path = Path(os.path.join(ROOT_DIR, "data", "output", "compiled.json"))
    output_path = Path(os.path.join(ROOT_DIR, "data", "output", "validated.json"))

    with open(input, "r") as fin, open(output_path) as fout:
        input_data = json.loads(fin.read())
        fout.write(json.dumps(process_json_data(input_data), indent=4))
        