import gspread
from google.oauth2.service_account import Credentials
import time
import random

import os
import json
import tempfile

import os
import json
import tempfile

def get_credentials():
    """
    Retrieve Google service account credentials from the environment variable.
    If the environment variable contains JSON, save it to a temporary file and return the file path.
    If the environment variable contains a file path, validate its existence and return it.
    """
    creds = os.getenv("GOOGLE_SERVICE_ACCOUNT_CREDS")
    if not creds:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_CREDS environment variable is not set.")

    # Check if the creds is a valid file path
    if os.path.exists(creds):
        return creds

    # Assume it's a JSON string, validate and save it to a temporary file
    try:
        creds_json = json.loads(creds)
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        temp_file.write(json.dumps(creds_json).encode())
        temp_file.close()
        return temp_file.name
    except json.JSONDecodeError as e:
        raise ValueError(f"GOOGLE_SERVICE_ACCOUNT_CREDS does not contain valid JSON or a valid file path. Error: {e}")

class GoogleSheetManager:
    def __init__(self, creds_file, sheet_id, scopes):
        self.creds_file = get_credentials()
        self.sheet_id = sheet_id
        self.scopes = scopes
        self.worksheet = self.connect_to_sheet()

    def connect_to_sheet(self):
        """
        Connects to Google Sheets using service account credentials.
        """
        creds = Credentials.from_service_account_file(self.creds_file, scopes=self.scopes)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(self.sheet_id)
        return sheet.sheet1  # Access the first sheet
    
    def clear_sheet(self):
        """
        Clears the current worksheet.
        """
        try:
            self.worksheet.clear()
            print("Cleared sheet.")
        except gspread.exceptions.APIError as e:
            print(f"Error clearing Google Sheet: {e}")
            raise e
    
    @staticmethod
    def exponential_backoff(self, retries):
        """
        Implements exponential backoff for retrying API calls.
        """
        return min(60, (2 ** retries) + random.random())

    def append_rows(self, rows, max_retries=5):
        """
        Appends rows to the current worksheet, with exponential backoff to handle rate limits.
        """
        retries = 0
        while retries < max_retries:
            try:
                self.worksheet.append_rows(rows)
                print(f"Successfully inserted {len(rows)} rows.")
                break  # Success, exit loop
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:  # Rate limit exceeded
                    wait_time = self.exponential_backoff(retries)
                    print(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise e  # Re-raise the exception for other errors
            if retries == max_retries:
                print(f"Failed to insert rows after {max_retries} retries.")

    def delete_rows(self, row_indices, max_retries=5):
        """
        Deletes specified rows from the worksheet, with exponential backoff to handle rate limits.
        """
        retries = 0
        while retries < max_retries:
            try:
                for row in sorted(row_indices, reverse=True):
                    self.worksheet.delete_rows(row)
                print(f"Successfully deleted rows.")
                break  # Success, exit loop
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:  # Rate limit exceeded
                    wait_time = self.exponential_backoff(retries)
                    print(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise e  # Re-raise the exception for other errors
            if retries == max_retries:
                print(f"Failed to delete rows after {max_retries} retries.")
                
    def get_all_data(self):
        """
        Retrieves all data from the worksheet.
        """
        return self.worksheet.get_all_values()

    def find_rows_by_org_id(self, org_id, org_id_column_index=1):
        """
        Finds all rows that match a specific org_id in the sheet.
        Checks if the row has enough columns to access org_id_column_index
        Returns a list of row indices to be deleted.
        """
        all_data = self.get_all_data()
        rows_to_delete = []

        for row_index, row in enumerate(all_data):
            if len(row) > org_id_column_index:
                if row[org_id_column_index] == org_id:
                    rows_to_delete.append(row_index + 1)
            else:
                print(f"Row {row_index + 1} is too short: {row}")
        return rows_to_delete
    
    def delete_rows_by_org_id(self, org_id, org_id_column_index=1, max_retries=5):
        """
        Deletes all rows that match a specific org_id, applying batch deletion for consecutive rows.
        Applies exponential backoff for handling rate limits.
        """
        # Step 1: Find all the rows that match the org_id
        rows_to_delete = self.find_rows_by_org_id(org_id, org_id_column_index)
        if not rows_to_delete:
            print(f"No rows found")
            return
        print(f"Found {len(rows_to_delete)} rows to delete")

        retries = 0
        while retries < max_retries:
            try:
                # Batch deletion: Delete consecutive ranges of rows
                if rows_to_delete:
                    start = rows_to_delete[0]
                    end = rows_to_delete[-1]
                    self.worksheet.delete_rows(start, end)  # Delete all consecutive rows in a range
                    print(f"Successfully deleted row {start} to {end}")
                break  # Exit loop after success
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:  # Rate limit exceeded
                    wait_time = self.exponential_backoff(retries)
                    print(f"Rate limit exceeded. Retrying in {wait_time:.2f} seconds... (Attempt {retries + 1}/{max_retries})")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise e  # Raise other exceptions
            if retries == max_retries:
                print(f"Failed to delete rows for org_id {org_id} after {max_retries} retries.")
