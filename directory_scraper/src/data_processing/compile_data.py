import json
import os
import logging

#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_SCHEMA = {
    #"id": {"type": str, "nullable": False},
    "org_sort": {"type": int, "nullable": False},
    "org_id": {"type": str, "nullable": False},
    "org_name": {"type": str, "nullable": False},
    "org_type": {"type": str, "nullable": False},
    "division_sort": {"type": int, "nullable": False},
    "division_name": {"type": str, "nullable": True},
    "subdivision_name": {"type": str, "nullable": True},
    "position_sort": {"type": int, "nullable": False},
    "person_name": {"type": str, "nullable": True},
    "position_name": {"type": str, "nullable": True},
    "person_phone": {"type": str, "nullable": True},
    "person_email": {"type": str, "nullable": True},
    "person_fax": {"type": str, "nullable": True},
    "parent_org_id": {"type": str, "nullable": True},
}

def validate_required_keys(record, json_file):
    """Function to validate required keys"""
    for key, meta in DATA_SCHEMA.items():
        if key not in record:
            logging.warning(f"Missing '{key}' in {json_file}, setting default value.")
            record[key] = None  # specify default value for missing fields

    return record

def compile_json_files(input_folder):
    """Function to compile all JSON files from a folder into one list"""
    compiled_data = []
    
    for json_file in os.listdir(input_folder):
        if json_file.endswith('.json'):
            json_file_path = os.path.join(input_folder, json_file)
            with open(json_file_path, 'r') as f:
                data = json.load(f)
                for record in data:
                    record = validate_required_keys(record, json_file)
                    compiled_data.append(record)
    
    return compiled_data

def sort_data(data):
    """Function to sort the compiled data"""
    return sorted(data, key=lambda x: (x['org_sort'], x['division_sort'], x['position_sort']))

def write_json_file(file_path, data):
    """Function to write data back to a JSON file"""
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)

def write_json_file_row_by_row(file_path, data):
    """
    Function to write data back to a JSON file in a row-by-row format,
    where each dictionary is written on its own line inside a valid JSON array.
    """
    with open(file_path, 'w') as file:
        # Write the opening bracket for the JSON array
        file.write('[\n')
        
        # Loop through each record in data and write it on a new line
        for index, record in enumerate(data):
            json.dump(record, file, separators=(',', ':'), ensure_ascii=False)
            # Add a comma after each object except the last one
            if index < len(data) - 1:
                file.write(',\n')
            else:
                file.write('\n')
        
        # Write the closing bracket for the JSON array
        file.write(']')

def data_compiling_pipeline(input_folder, output_file):
    """Main pipeline function"""
    compiled_data = compile_json_files(input_folder)
    sorted_data = sort_data(compiled_data)
    write_json_file_row_by_row(output_file, sorted_data)
    
    logging.info(f"Successfully compiled! Saved as {output_file}")


if __name__ == "__main__":
    input_folder = 'data/output'
    output_file = 'compiled.json'

    data_compiling_pipeline(input_folder, output_file)