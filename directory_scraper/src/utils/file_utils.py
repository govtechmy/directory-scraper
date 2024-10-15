import json
import os

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
