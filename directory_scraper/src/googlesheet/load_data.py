# main_logic.py
from datetime import datetime
from google_sheets_utils import GoogleSheetManager
from process_data import group_data_by_org_id, add_timestamp_to_rows

def load_data_into_sheet(google_sheets_manager, data, add_timestamp=True):
    """
    Insert data into Google Sheet.
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
        header.append('last_updated')
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