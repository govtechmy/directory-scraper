import os
import shutil
import logging
from process_data import process_all_json_files 
from compile_data import data_compiling_pipeline 

logging.basicConfig(level=logging.WARNING)

if __name__ == "__main__":
    input_folder = 'data/input' #define
    output_folder = 'data/output' #define

    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
        logging.info(f"Cleared the existing '{output_folder}' folder.")

    os.makedirs(output_folder)
    logging.info(f"Created a new empty '{output_folder}' folder.")

    process_all_json_files(input_folder, output_folder)

    output_file = 'compiled.json' #define

    data_compiling_pipeline(output_folder, output_file)
