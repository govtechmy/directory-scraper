import os
import json
import re
import uuid
import shutil
import logging
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.src.data_processing.schema import SCHEMA_MAPPING, SCHEMA_REGISTRY
from directory_scraper.src.data_processing.utils.utils_process import (
    load_json,
    save_json
)

logger = logging.getLogger(__name__)
logging.basicConfig(
    filename="process_data.log",
    filemode="w",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def process_json_file(json_file_path, output_folder, schema_name):
    """
    Processes a single JSON file with a specified schema.
    """
    json_file_name = os.path.basename(json_file_path)

    try:
        data = load_json(json_file_path)
        processor_class = SCHEMA_REGISTRY[schema_name]  # Lookup processor
        processor = processor_class()

        if isinstance(data, list):
            processed_data = processor.process_pipeline(data)

            # Save processed data in the same subfolder structure
            subfolder = os.path.basename(os.path.dirname(json_file_path))
            output_subfolder = os.path.join(output_folder, subfolder)
            os.makedirs(output_subfolder, exist_ok=True)

            save_json(processed_data, json_file_name, output_subfolder)
            logging.info(f"Processed and saved: {json_file_name} to {output_subfolder}")
        else:
            logging.warning(f"Invalid JSON format in {json_file_name}, skipping file.")
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON in {json_file_name}. Skipping file.")
    except Exception as e:
        logging.error(f"An error occurred while processing {json_file_name}: {str(e)}")
        raise e

def process_all_json_files(input_folder, output_folder):
    """
    Processes all JSON files in the input folder.
    Matches the schema based on the subfolder structure.
    """
    if not os.path.exists(input_folder):
        logging.error(f"The folder '{input_folder}' does not exist.")
        return

    total_files = 0
    skipped_files = 0

    for root, dirs, files in os.walk(input_folder):
        # Determine schema based on the subfolder name
        folder_name = os.path.basename(root)
        schema_name = SCHEMA_MAPPING.get(folder_name)

        if not schema_name:
            logging.warning(f"No matching schema for folder: {folder_name}. Skipping.")
            continue

        # Process each JSON file in the current folder
        for json_file in files:
            if json_file.endswith('.json'):
                json_file_path = os.path.join(root, json_file)
                total_files += 1
                try:
                    process_json_file(json_file_path, output_folder, schema_name)
                except Exception:
                    skipped_files += 1

    logging.info(f"Processing summary: {total_files} files processed, {skipped_files} files skipped.")

if __name__ == "__main__":
    input_folder = 'data/spiders_output'
    output_folder = 'data/clean_data'

    # Clear and recreate the output folder
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)  # Remove all files and subdirectories
        logging.info(f"Cleared the existing '{output_folder}' folder.")
        print(f"Cleared the existing '{output_folder}' folder.")

    os.makedirs(output_folder)  # Recreate the empty folder
    logging.info(f"Created a new empty '{output_folder}' folder.")
    process_all_json_files(input_folder, output_folder)
