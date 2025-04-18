import json
import os
import logging

def load_json_file(file_name):
    """
    Generic function to load any JSON file from the utils/json directory.
    """
    file_path = os.path.join(os.path.dirname(__file__), 'json', file_name)  # Adjust path to json subfolder
    with open(file_path, "r") as f:
        return json.load(f)

def load_mindef_units():
    """
    Loads and returns the content of mindef_units.json.
    """
    return load_json_file('mindef_units.json')

def load_org_mapping():
    """Load and return the org_mapping.json file."""
    org_mapping_file = os.path.join(os.path.dirname(__file__), 'json', 'org_mapping.json')
    try:
        with open(org_mapping_file, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        logging.error(f"Error: Mapping file '{org_mapping_file}' not found.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Error: Could not decode JSON in '{org_mapping_file}'.")
        return None
    
def load_spreadsheets_config():
    """
    Loads the spreadsheets configuration from the 'gsheets_config.json' file.
    """
    spreadsheets_config_file = os.path.join(os.path.dirname(__file__), 'json', 'gsheets_config.json')
    try:
        with open(spreadsheets_config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {spreadsheets_config_file} not found.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Failed to parse {spreadsheets_config_file}. Please ensure it is valid JSON.")
        return {}