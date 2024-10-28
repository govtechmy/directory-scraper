from datetime import datetime
from google_sheets_utils import GoogleSheetManager
from process_data import group_data_by_org_id, add_timestamp_to_rows

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
