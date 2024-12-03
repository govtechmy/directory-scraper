import os
import sys
import logging
from pathlib import Path
from fastapi import FastAPI

PATH_ROOT = Path(__file__).parents[4]
sys.path.append(str(PATH_ROOT))

from directory_scraper.src.google_sheets_api.utils.utils_validate import clean_data
from directory_scraper.src.google_sheets_api.utils.utils_gsheet import GoogleSheetManager
from directory_scraper.src.elasticsearch_upload.data_to_es import main as elasticsearch_upload


logger = logging.getLogger(__name__)
logging.basicConfig(filename="gsheet_api.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(pathname)s - Line %(lineno)d - %(levelname)s - %(message)s')

app = FastAPI(
    title="DIRECTORY-SCRAPER-API",
)

@app.get("/")
async def root():
    return {"message": "Hello World!"}

@app.get("/gsheet/setup_logs")
async def setup_logs(sheet_id:str) -> dict:
    """
    Function to setup google sheet edit log worksheet
    """
    creds = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

    try:
        sheet_manager = GoogleSheetManager(creds, sheet_id, scopes)
        sheet_manager.setup_editsheet()
    except Exception as e:
        logging.error("Unable to create 'Edit Logs' sheet for google sheet. Check your `sheet_id` and ensure it is correct.")
        raise e
    
    return {"sheet_id": sheet_id}

@app.get("/gsheet/upload_data")
async def upload_to_elasticsearch(sheet_id: str, user_name: str) -> dict:
    """
    Function to extract data from google sheet, detect changes and upload the data to elasticsearch.
    Also sets up edit logs if they don't exist and uploads person_email, latest document_hash, sheet_id and timestamp
    to the edit logs.
    """
    try:
        gsheet_data = clean_data(sheet_id, user_name)
        cleaned_data = gsheet_data.get("cleaned_data")
        document_hash = gsheet_data.get("document_hash")
        previous_hash = gsheet_data.get("previous_hash")
        current_hash = gsheet_data.get("current_hash")

        if previous_hash == current_hash:
            logging.info("Changes detected, cleaning data and uploading to ElasticSearch")
            elasticsearch_upload(cleaned_data)

    except Exception as e:
        logging.error(e)
        return {"document_hash": None, "previous_hash": None, "current_hash": None, "sheet_id": sheet_id}
    
    return {"document_hash": document_hash, "previous_hash": previous_hash, "current_hash": document_hash, "sheet_id": sheet_id}