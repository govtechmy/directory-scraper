# data_processor.py
import json
from datetime import datetime

def validate_data(json_data_path):
    """
    Validates and loads data from a JSON file.
    Returns the data if it's valid.
    Raises an error if the data format is incorrect.
    """
    try:
        with open(json_data_path, 'r') as json_file:
            data = json.load(json_file)
        if not data or not isinstance(data, list):
            raise ValueError("Invalid data format: The JSON file must contain a list of dictionaries.")
        print("Successfully validate JSON data.")
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
