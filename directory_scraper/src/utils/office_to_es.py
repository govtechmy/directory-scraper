import os
import time
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from directory_scraper.src.google_sheets.fetch_gsheets import exponential_backoff
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager

load_dotenv()

# Google Sheets API credentials
CREDS_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDS")
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def clean_office_data(sheet_id, max_retries=5):
    retries = 0
    while retries <= max_retries:
        try:
            # Initialize Google Sheet Manager
            sheet_manager = GoogleSheetManager(CREDS_FILE, sheet_id, SCOPES)

            # Fetch all data
            data_df = pd.DataFrame(sheet_manager.get_all_data())

            # Process column names and data rows
            socmed_sites = {"facebook", "twitter", "instagram", "tiktok", "youtube", "whatsapp"} 
            
            data_df.columns = [
                "id", "name", "line1", "line2", "line3", "postcode", "state",
                "phone", "fax", "org_id2", "email", "website", "social_media"
            ]
            data_df = data_df.loc[1:, :].drop("org_id2", axis=1)
            data_df = data_df.assign(address=data_df.loc[:, ["line1", "line2", "line3", "postcode", "state"]].to_dict(orient="records"))
            data_df = data_df.assign(contact=data_df.loc[:, ["phone", "fax", "email", "website"]].to_dict(orient="records"))
            data_df["social_media"] = data_df["social_media"].apply(lambda x: {socmed.strip(): url.strip()
                for (socmed, url)
                in [line.split(":", maxsplit=1)
                    for line
                    in x.split("\n")
                    if set(line.lower().split(":")).intersection(socmed_sites)]
            })
            data_df = data_df.assign(operating_hours="-")
            office_json = data_df.loc[:, ["id", "name", "address", "contact", "social_media", "operating_hours"]].to_dict(orient="records")

            return office_json

        except Exception as e:
            if "429" or "503" in str(e):
                if retries < max_retries:
                    wait_time = exponential_backoff(retries)
                    print(f"Quota exceeded for Directory - office. Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    print(f"Failed to fetch sheet Directory - office: Quota exceeded after {max_retries} retries.")
                    return "failed", f"Directory - office: Retry/Quota Exceeded/Gsheet Id"
            else:
                print(f"Error fetching or storing data for file_name=Directory - office: {e}")
                return "failed", f"Directory - office: Error ({e})"