import gspread
from google.oauth2.service_account import Credentials
import json
import time
import random
from dotenv import load_dotenv
import os

load_dotenv()

GOOGLE_SERVICE_ACCOUNT_CREDS = os.getenv('GOOGLE_SERVICE_ACCOUNT_CREDS')
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
JSON_DATA_PATH = 'data/data.json'
BATCH_SIZE = 500  # Batch size for inserting rows


def connect_to_google_sheet(GOOGLE_SERVICE_ACCOUNT_CREDS, SCOPES, GOOGLE_SHEET_ID):
    """
    Connects to Google Sheets using a service account and returns the worksheet object.
    """
    try:
        creds = Credentials.from_service_account_file(GOOGLE_SERVICE_ACCOUNT_CREDS, scopes=SCOPES)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.sheet1  # Access the first sheet (can be changed to access by name)
        print("Successfully connected to Google Sheet.")
        return worksheet
    except Exception as e:
        print(f"Error connecting to Google Sheet: {e}")
        raise

def validate_data(JSON_DATA_PATH):
    """
    Validates and loads data from a JSON file.
    Returns the data if it's valid.
    """
    try:
        with open(JSON_DATA_PATH, 'r') as json_file:
            data = json.load(json_file)
        if not data or not isinstance(data, list):
            raise ValueError("Invalid data format: The JSON file must contain a list of dictionaries.")
        print("Successfully load and validate JSON data.")
        return data
    except Exception as e:
        print(f"Error loading or validating JSON data: {e}")
        raise

def group_data_by_org_id(data):
    """
    Groups data by 'org_id' and returns a dictionary where the key is the org_id and the value is the data list.
    """
    grouped_data = {}
    for row in data:
        org_id = row['org_id']
        if org_id not in grouped_data:
            grouped_data[org_id] = []
        grouped_data[org_id].append(row)
    return grouped_data

def exponential_backoff(retries):
    """
    Exponential backoff logic to handle rate limits. GCP recommendation.
    """
    return min(60, (2 ** retries) + random.random())

def clear_sheet(worksheet):
    """
    Clears the Google Sheet once at the beginning of the process.
    """
    try:
        worksheet.clear()
        print("Google Sheet cleared successfully.")
    except Exception as e:
        print(f"Error clearing Google Sheet: {e}")
        raise

def insert_data_to_sheet(worksheet, data):
    """
    Inserts data into the Google Sheet in batches.
    Writes the header and then inserts the data in batches.
    Implements exponential backoff to handle rate limits and prints progress.
    """
    try:
        # Extract header and rows from the data
        header = list(data[0].keys())
        rows = [list(item.values()) for item in data]

        total_rows = len(rows)
        print(f"Total rows: {total_rows}")

        # Insert data in batches
        for i in range(0, total_rows, BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            retries = 0
            while retries < 5:  # Retry up to 5 times for each batch
                try:
                    worksheet.append_rows(batch)
                    print(f"Inserted batch {i // BATCH_SIZE + 1}")
                    break  # Break the retry loop if successful
                except gspread.exceptions.APIError as e:
                    if e.response.status_code == 429:  # Rate limit error
                        wait_time = exponential_backoff(retries)
                        print(f"Rate limit exceeded, retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/5)")
                        time.sleep(wait_time)
                        retries += 1
                    else:
                        raise e
            if retries == 5:
                print(f"Failed to insert batch {i // BATCH_SIZE + 1} after multiple retries.")

        print(f"Successfully written data to Google Sheets.")
    except Exception as e:
        print(f"Error inserting data: {e}")
        raise

def main():
    worksheet = connect_to_google_sheet(GOOGLE_SERVICE_ACCOUNT_CREDS, SCOPES, GOOGLE_SHEET_ID)
    
    clear_sheet(worksheet)
    
    data = validate_data(JSON_DATA_PATH)
    
    grouped_data = group_data_by_org_id(data)
    
    total_orgs = len(grouped_data)
    print(f"Total number of org_id to process: {total_orgs}")

    # Write the header once only, before inserting any data
    header = list(data[0].keys())
    worksheet.append_row(header)
    
    # Process each org_id group individually with a counter
    for org_index, (org_id, group_data) in enumerate(grouped_data.items(), start=1):
        print(f"\nProcessing org_id: {org_id} ({org_index}/{total_orgs})")
        insert_data_to_sheet(worksheet, group_data)
        print(f"Completed processing for org_id: {org_id}")

if __name__ == '__main__':
    main()
