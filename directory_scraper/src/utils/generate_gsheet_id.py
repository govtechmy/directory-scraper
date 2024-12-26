"""
This script performs the following tasks:
1. Creates Google Sheets for each `spider_name` in the mapping.
2. Moves the created sheets to a specified Google Drive folder.
3. Makes the sheets public with read-only access.
4. Saves & output the sheet ID mapping (`secret_gsheet_id.json`) and configuration (`gsheets_config.json`).
- `gsheets_config.json` should be stored in src/utils/json and is crucial for use of loading data_file to gsheets, and fetching gsheets.
- `secret_gsheet_id` content should be paste in .env GSHEET_ID_MAPPING
"""

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
import json
import time
import random
from dotenv import load_dotenv

load_dotenv()

# Mapping of spider_name to org_id
spider_to_org_mapping = {
    "digital": "DIGITAL",
    "ekonomi": "EKONOMI",
    "jpm": "JPM",
    "kbs": "KBS",
    "kkr": "KKR",
    "kln": "KLN",
    "komunikasi": "KOMUNIKASI",
    "kpdn_negeri": "KPDN",
    "kpdn": "KPDN",
    "kpk": "KPK",
    "kpkm": "KPKM",
    "kpkt": "KPKT",
    "kpn": "KPN",
    "kpt": "KPT",
    "kpwkm": "KPWKM",
    "kuskop": "KUSKOP",
    "miti": "MITI",
    "mod": "MOD",
    "moe": "MOE",
    "mof": "MOF",
    "moh": "MOH",
    "moha": "MOHA",
    "mohr": "MOHR",
    "mosti": "MOSTI",
    "mot": "MOT",
    "motac": "MOTAC",
    "nres": "NRES",
    "petra": "PETRA",
    "rurallink_anggota": "RURALLINK",
    "rurallink_pkd": "RURALLINK"
}

SCOPES = ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"]
CREDS_FILE = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
GOOGLE_DRIVE_FOLDER_ID = os.getenv('GOOGLE_DRIVE_FOLDER_ID')

creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
drive_service = build("drive", "v3", credentials=creds)

if not GOOGLE_DRIVE_FOLDER_ID:
    print("ERROR: The environment variable 'GOOGLE_DRIVE_FOLDER_ID' is not set.")
    exit(1) 

def make_sheet_public(sheet_id, role="reader"):
    """
    Makes the Google Sheet accessible to anyone with the link.
    Args:
        sheet_id (str): The ID of the Google Sheet.
        role (str): The permission role (e.g., "reader" or "writer").
    """
    try:
        drive_service.permissions().create(
            fileId=sheet_id,
            body={
                "type": "anyone",
                "role": role,  # "reader" for read-only, "writer" for edit access
            },
            fields="id",
        ).execute()
        # print(f"Made sheet {sheet_id} public with {role} access.")
    except Exception as e:
        print(f"Error making sheet {sheet_id} public: {e}")

def exponential_backoff(func, *args, max_retries=5, maximum_backoff=32, **kwargs):
    retries = 0
    while retries < max_retries:
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            response = e.response.json()
            error_code = response.get("error", {}).get("code")
            error_reason = response.get("error", {}).get("errors", [{}])[0].get("reason", "")

            if error_code == 403 and error_reason in ["rateLimitExceeded", "userRateLimitExceeded"]:
                wait_time = min((2 ** retries) + random.uniform(0, 1), maximum_backoff)
                print(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                time.sleep(wait_time)
                retries += 1
            else:
                raise e
    raise Exception(f"Function {func.__name__} failed after {max_retries} retries due to rate limits.")

def move_sheet_to_folder(sheet_id, folder_id):
    """Move Gsheets to Google Drive Folder"""
    def move():
        file = drive_service.files().get(fileId=sheet_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))
        drive_service.files().update(
            fileId=sheet_id,
            addParents=folder_id,
            removeParents=previous_parents,
            fields="id, parents"
        ).execute()
    try:
        exponential_backoff(move)
        # print(f"Moved sheet {sheet_id} to folder {folder_id}")
    except Exception as e:
        print(f"Error moving sheet {sheet_id} to folder {folder_id}: {e}")

def create_google_sheets(spider_to_org_mapping, prefix="Direktori Gov"):
    """Create Google Sheets for each spider name"""
    secret_gsheet_id = {}
    gsheets_config = []

    for spider_name, org_id in spider_to_org_mapping.items():
        sheet_name = f"{prefix} - {spider_name}"
        
        try:
            # Create the sheet with exponential backoff
            def create_sheet():
                return client.create(sheet_name)

            sheet = exponential_backoff(create_sheet)
            sheet_id = sheet.id
            
            # Move the sheet to the target folder
            move_sheet_to_folder(sheet_id, GOOGLE_DRIVE_FOLDER_ID)
            
            # Make the sheet public
            make_sheet_public(sheet_id, role="reader")
            
            # Add to mappings
            secret_gsheet_id[spider_name] = sheet_id
            gsheets_config.append({
                "ref_name": spider_name,
                "org_id": org_id,
                "data_file": f"{spider_name}.json"
            })
            
            print(f"Created & moved: {sheet_name}, ID: {sheet_id}")
        except Exception as e:
            print(f"Error creating, moving, or sharing sheet '{sheet_name}': {e}")
            continue

    return secret_gsheet_id, gsheets_config

secret_gsheet_id, gsheets_config = create_google_sheets(spider_to_org_mapping)

# Save the sheet name to ID mapping
sheet_mapping_file = "secret_gsheet_id.json"
with open(sheet_mapping_file, "w") as f:
    json.dump(secret_gsheet_id, f, indent=4)
print(f"Saved sheet name to gsheet_id mapping to '{sheet_mapping_file}'")

# Save the org_id to data_file mapping
org_mapping_file = "gsheets_config.json"
with open(org_mapping_file, "w") as f:
    json.dump(gsheets_config, f, indent=4)
print(f"Saved org_id to data_file mapping to '{org_mapping_file}'")
