import json
from datetime import datetime
import time
from directory_scraper.src.google_sheets.google_sheets_utils import GoogleSheetManager
import os
from collections import defaultdict
import random
from directory_scraper.src.utils.discord_bot import send_discord_notification

DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL') 
THREAD_ID = os.getenv('THREAD_ID')

def validate_data(json_data):
    """
    Validates and loads data from a JSON file or validates an already loaded JSON object.
    Returns the data if it's valid.
    Raises an error if the data format is incorrect.
    """
    try:
        if isinstance(json_data, str):
            with open(json_data, 'r') as json_file:
                data = json.load(json_file)
        else:
            data = json_data  # Use directly if it's already loaded
        if not data or not isinstance(data, list):
            raise ValueError("Invalid data format: The JSON data must contain a list of dictionaries.")
        return data
    except Exception as e:
        print(f"Error loading or validating JSON data: {e}")
        raise


def group_data_by_org_id(data):
    """
    Groups data by 'org_id' and returns a dictionary where the key is the org_id
    and the value is the list of data for that organization.
    """
    grouped_data = {}
    for row in data:
        org_id = row['org_id']
        if org_id not in grouped_data:
            grouped_data[org_id] = []
        grouped_data[org_id].append(row)
    return grouped_data

def add_timestamp_to_rows(rows, add_timestamp=True):
    """
    Adds a timestamp column to each row.
    If add_timestamp is False, the function just returns the rows unchanged.
    """
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return [row + [timestamp] for row in rows]
    return rows

#===========================================================================
# Refactor load_data.py & update_data.py as a single file in process_data.py

def retry_with_backoff(func, *args, retries=5, base_delay=2, manager=None, **kwargs):
    """
    Retry a function with exponential backoff, using the GoogleSheetManager's backoff logic.
    """
    delay = base_delay
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "429" in str(e):  # Quota exceeded error
                # Use GoogleSheetManager's exponential_backoff if available
                if manager and hasattr(manager, "exponential_backoff"):
                    delay = manager.exponential_backoff(attempt)
                else:
                    delay = min(60, (2 ** attempt) + random.random())  # Fallback logic
                
                print(f"Quota exceeded, retrying in {delay:.2f} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries exceeded")
    
def load_data_into_sheet(google_sheets_manager, data, separate_grouped_data_by_sheet, add_timestamp=True, group_key="division_name"):
    """
    Load data into Google Sheets.

    Args:
        google_sheets_manager: An instance of GoogleSheetManager for managing Google Sheets.
        data: List of dictionaries representing the data to upload.
        add_timestamp: Whether to append a timestamp to each row.
        separate_grouped_data_by_sheet: Whether to separate the data into sheets grouped by `group_key`.
        group_key: The key to use for grouping data (if `separate_grouped_data_by_sheet` is True).
    """
    grouped_data = group_data_by_org_id(data)
    total_orgs = len(grouped_data)
    print(f"Total org_id found in data: {total_orgs}")
    row_count_summary = {}

    # Write the header once, before inserting any data
    header = list(data[0].keys())
    if add_timestamp:
        header.append('last_uploaded')

    retry_with_backoff(google_sheets_manager.switch_to_sheet, "Keseluruhan Direktori")
    retry_with_backoff(google_sheets_manager.clear_sheet)
    retry_with_backoff(google_sheets_manager.append_rows, [header])

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S') if add_timestamp else None

    # Process and insert data into the main sheet
    for org_index, (org_id, group_data) in enumerate(grouped_data.items(), start=1):
        print(f"Loading org_id: {org_id} ({org_index}/{total_orgs})")
        start_time = datetime.now()
        group_rows = [list(item.values()) for item in group_data]
        if add_timestamp:
            group_rows = [row + [timestamp] for row in group_rows]
        retry_with_backoff(google_sheets_manager.append_rows, group_rows)
        row_count_summary[org_id] = len(group_rows)

    print(f"Full data loaded into the main sheet.")

    if separate_grouped_data_by_sheet:
        print(f"\nSeparating data by '{group_key}' and loading into separate sheets...")
        grouped_by_key = defaultdict(list)
        for row in data:
            key_value = row.get(group_key, "Unknown")
            grouped_by_key[key_value].append(row)

        total_groups = len(grouped_by_key)
        print(f"Total groups found for {group_key}: {total_groups}")

        # Process each group and load them into separate sheets
        for index, (key_value, group_rows) in enumerate(grouped_by_key.items(), start=1):
            sheet_name = f"{key_value}"
            print(f"\nUploading group - {org_id}: '{key_value}' ({index}/{total_groups}) ...")

            try:
                retry_with_backoff(google_sheets_manager.switch_to_sheet, sheet_name)
                retry_with_backoff(google_sheets_manager.clear_sheet)
                retry_with_backoff(google_sheets_manager.append_rows, [header])

                # Convert group rows to values and append
                rows_to_append = [list(row.values()) for row in group_rows]
                if add_timestamp:
                    rows_to_append = [row + [timestamp] for row in rows_to_append]
                google_sheets_manager.append_rows(rows_to_append)
            except Exception as e:
                if DISCORD_WEBHOOK_URL:
                    send_discord_notification(f"\n❗ Error uploading group - {org_id} - '{key_value}']: {e}", DISCORD_WEBHOOK_URL, THREAD_ID)
            
                print(f"❗ Error uploading group '{key_value}' to sheet '{sheet_name}': {e}")

            end_time = datetime.now()
            duration = end_time - start_time

            if DISCORD_WEBHOOK_URL:
                send_discord_notification(f"\n({index}/{total_groups}) {key_value} [Duration: {duration}]", DISCORD_WEBHOOK_URL, THREAD_ID)

        end_time = datetime.now()
        duration = end_time - start_time

        if DISCORD_WEBHOOK_URL:
            send_discord_notification(f"\nFinished: {org_id} [Total Duration: {duration}]", DISCORD_WEBHOOK_URL, THREAD_ID)

    return row_count_summary

def update_data_in_sheet(google_sheets_manager, data, add_timestamp=True):
    """
    Update data in Google Sheet.
    1. Identify org_id to be updated.
    2. Delete all rows for the org_id.
    3. Insert/Append new rows for the org_id, including timestamp.
    """
    grouped_data = group_data_by_org_id(data)
    total_orgs = len(grouped_data)
    print(f"Total number of org_id to update: {total_orgs}")

    # Generate a single timestamp for all rows if needed
    timestamp = None
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Define the header
    header = list(data[0].keys())  # Extract the keys as headers
    if add_timestamp:
        header.append('last_uploaded')  # Append the timestamp header
    
    # Check if the sheet is empty and add the header
    all_data = google_sheets_manager.get_all_data()
    
    # Handle cases where all_data contains only empty rows like [[]]
    if not all_data or all(len(row) == 0 for row in all_data):
        print("Sheet is empty. Adding header row...")
        google_sheets_manager.append_rows([header])  # Add header if sheet is blank

    for org_index, (org_id, group_data) in enumerate(grouped_data.items(), start=1):
        print(f"\nUpdating org_id: {org_id} ({org_index}/{total_orgs})")
        
        # Convert group_data to rows
        group_rows = [list(item.values()) for item in group_data]

        # Add timestamp to each row if needed
        if add_timestamp:
            group_rows = [row + [timestamp] for row in group_rows]

        # Step 1: Delete all rows for this org_id
        google_sheets_manager.delete_rows_by_org_id(org_id)

        # Step 2: Insert new rows for this org_id
        google_sheets_manager.append_rows(group_rows)

def get_gsheet_id(ref_name):
    """Retrieve the Google Sheet ID for reference name (org_id) from the GSHEET_ID_MAPPING environment variable."""
    gsheet_id_mapping = os.getenv("GSHEET_ID_MAPPING")
    if not gsheet_id_mapping:
        raise ValueError("Environment variable GSHEET_ID_MAPPING is not set.")

    try:
        mapping = json.loads(gsheet_id_mapping)
        gsheet_id = mapping.get(ref_name)
        if not gsheet_id:
            raise ValueError(f"Sheet ID not found for org_id: {ref_name}")
        return gsheet_id
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to decode GSHEET_ID_MAPPING. Ensure it is valid JSON. Error: {e}")