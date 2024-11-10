import json
from datetime import datetime
from google_sheets_utils import GoogleSheetManager

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
        print("Successfully validated JSON data.")
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

def load_data_into_sheet(google_sheets_manager, data, add_timestamp=True):
    """
    Load data into Google Sheet.
    1. Identify org_id to be inserted.
    2. Add header
    3. Insert/Append rows for the org_id, including timestamp.
    """
    grouped_data = group_data_by_org_id(data)
    total_orgs = len(grouped_data)
    print(f"Total number of org_id to process: {total_orgs}")

    # Write the header once, before inserting any data
    header = list(data[0].keys())
    if add_timestamp:
        header.append('last_uploaded')
    google_sheets_manager.append_rows([header])  # Append the header to the sheet

    # Generate a single timestamp for all rows if needed
    timestamp = None
    if add_timestamp:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Process each org_id group and insert the data in batches
    for org_index, (org_id, group_data) in enumerate(grouped_data.items(), start=1):
        print(f"\nLoading org_id: {org_id} ({org_index}/{total_orgs})")
        
        # Convert group_data to rows
        group_rows = [list(item.values()) for item in group_data]

        # Add timestamp to each row if needed
        if add_timestamp:
            group_rows = [row + [timestamp] for row in group_rows]

        # Insert the rows into Google Sheets with exponential backoff
        google_sheets_manager.append_rows(group_rows)

        print(f"Completed inserting data for org_id: {org_id}")

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
        
        print(f"Completed updating data for org_id: {org_id}")
