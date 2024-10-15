import os
import json
import re
import uuid
import shutil
import logging
from utils.file_utils import load_org_mapping

logger = logging.getLogger(__name__)
logging.basicConfig(filename="process_data.log", filemode="w", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_SCHEMA = {
    "org_sort": {"type": int, "nullable": False},
    "org_id": {"type": str, "nullable": False},
    "org_name": {"type": str, "nullable": False},
    "org_type": {"type": str, "nullable": False},
    "division_sort": {"type": int, "nullable": False},
    "division_name": {"type": str, "nullable": True},
    "subdivision_name": {"type": str, "nullable": True},
    "person_name": {"type": str, "nullable": True},
    "position_name": {"type": str, "nullable": True},
    "person_phone": {"type": str, "nullable": True},
    "person_email": {"type": str, "nullable": True},
    "person_fax": {"type": str, "nullable": True},
    "parent_org_id": {"type": str, "nullable": True},
    #"position_sort_order": {"type": int, "nullable": False}
}

UPPERCASE_KEYS = ["org_name", "division_name", "subdivision_name", "person_name","position_name"]
TITLECASE_KEYS = [""]

EMAIL_REGEX = r'^[a-zA-Z0-9_.+\'\`-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'

ALLOWED_ORG_TYPES = ["ministry", "agency"]

# NOTE these regex are used with re.IGNORECASE
VALID_PHONE_REGEX = [
    r'^\+60 1\d-\d{3} \d{4}$',    # +60 1x-xxx xxxx
    r'^\+60 11-\d{4} \d{4}$',     # +60 11-xxxx xxxx
    r'^\+60 3-\d{4} \d{4}$',      # +60 3-xxxx xxxx
    r'^\+60 8\d-\d{3} \d{3}$',    # +60 8x-xxx xxx
    r'^\+60 \d-\d{3} \d{4}$',     # +60 x-xxx xxxx
    r'^01\d-\d{3} \d{4}$',        # 01x-xxx xxxx
    r'^011-\d{4} \d{4}$',         # 011-xxxx xxxx
    r'^03-\d{4} \d{4}$',          # 03-xxxx xxxx (with space)
    r'^03-\d{8}$',                # 03-xxxxxxxx (no space)
    r'^03-\d{9}$',                # 03-xxxxxxxxx (9 digits after 03)
    r'^08\d-\d{3} \d{3}$',        # 08x-xxx xxx
    r'^0\d-\d{3} \d{4}$',         # 0x-xxx xxxx (with space)
    r'^0\d-\d{7}$',               # 0x-xxxxxxx (no space)
    r'^603-\d{7,8}$',             # 603-XXXXXXX or 603-XXXXXXXX (no space after 603)
    r'^\+603-\d{7,8}$',           # +603-XXXXXXX or +603-XXXXXXXX
    r'^\+32\d{8,9}$',             # +32XXXXXXXX or +32XXXXXXXXX (Belgium)
    r'^\+86\d{9,10}$',            # +86XXXXXXXXX or +86XXXXXXXXXX (China)
    r'^\+91\d{9,10}$',            # +91XXXXXXXXX or +91XXXXXXXXXX (India)
    r'^\+62-\d{1,5}-\d{4,}$',     # +62-XXXXX-XXXXX (Indonesia)
    r'^\+65\d{8}$',               # +65XXXXXXXX (Singapore)
    r'^\(\+41\)\d{9}$',           # (+41)XXXXXXXXX (Switzerland)
    r'^\(\+41\)022\d{7}$',        # (+41)022XXXXXXX (Switzerland with area code 022)
    r'^\(\+62\)\d{7,}$',          # (+62)XXXXXXXXX (Indonesia)
    r'^\+662-\d{4,7}$',           # +662-XXXXXXX (Thailand)
    r'^\(\+66\)\d{7,}$',          # (+66)XXXXXXXX (Thailand)
    r'^\(\+62\)\d{7,}\-\d{1,5}$', # (+62)XXXXXX-ext.XXXXX (Indonesia with extension)
    r'^\+1-\d{3}-\d{3}-\d{4}$',   # +1-XXX-XXX-XXXX (U.S. format)
    r'^\(\d{5}\)\d{7,}$',         # (xxxxx)XXXXXXXXX (International with parentheses)
    r'^\(\d{4}\)\d{7,}$',         # (xxxx)XXXXXXXXX (Vietnam etc.)
    r'^\(\d{4}\)\d{8,}$',         # (8424)XXXXXXXX (Vietnam with area code 8424)
    r'^\(\d{4}\)\d{7,}-\d{1,5}$', # (xxxx)XXXXX-XXXX (Extensions)
    r'^\(\d{4}\)\d{8}-\d{1,5}$',  # (8424)XXXXXXXX-XXXX (Vietnam with extension)
    r'^\d{3}-\d{6,7}$',           # xxx-xxxxxxx (Local numbers like 088-xxxxxx)
    r'^\d{3}-\d{7}$',             # xxx-xxxxxxx (e.g., 03-620000377)
    r'^0\d{1,2}-\d{6,7}$',        # 04-2625133 (Malaysian fixed-line format without ext.)
    r'^0\d{1,2}-\d{6,7}-ext\.\d{1,4}$',  # 04-2625133-ext.101 (Malaysian fixed-line with extension)
    r'^\d{8,}$',                  # 042625133 (without dash, should be formatted with dashes)
    r'^03-\d{9}$',                # 03-0388721983 (Malaysian number with extra digits)
    r'^\d{2}-\d{7}\(\d{3}\)$',    # 04-7314957(117) (Malaysian fixed-line with extension in parentheses)
    r'^\d{2}-\d{8}/\d{4}$',       # 03-27714325/4311 (Malaysian number with internal extension separated by '/')
    r'^\(\d{4}\)\d{8}/\d{4}$',    # (8424)37343849/3836 (Vietnam number with extension separated by '/')
    r'^03-\d{8}\(\d{4}\)$',       # 03-88823330(6330) (Malaysian number with extension in parentheses)
    r'^62-\d{9,10}$',             # 62-215224962 (Indonesian number starting with country code 62)
    r'^\(\+62\)\d{7,}-ext\.\d{4}/\(\+62\)\d{3}-\d{4}-\d{4}$',  # (+62)215224947-ext.3105/(+62)811-8881-0247 (Multiple Indonesian numbers with extension)
    r'^03-\d{9}$',                # 03-0388721901 (Malaysian number with extra digits)
    r'^0[35]-\d{6,8}sa?mb\.?\d{1,4}$',   # 03-55106922Samb.10 (Malaysian number with "Samb.", "Samb", "samb" and "smb" extension)
    r'^\d{2}-\d{7}\(\d{3}\)$',    # 088-254317(117) (Malaysian fixed-line with extension in parentheses)
    r'^\d{2}-\d{8}$',             # 04-73149579 (Malaysian fixed-line number with 8 digits)
    r'^\d{2}-\d{7}\(\d{3}\)$',    # 088-254317(133) (Malaysian number with extension)
    r'^\d{2,3}-\d{6,8}\(\d{1,5}\)$',  # Matches phone numbers like '088-254317(125)'
    r'^1800-\d{2}-\d{4}$',        # 1800-88-xxxx (Toll-free number)
    r'^\d{2,3}-\d{7}\(\d{1,5}\)$', # 082-232434(285) (Local number with extension)
    r'^\+61\d{1}\d{8,9}$',        # +612XXXXXXXX or +612XXXXXXXXX (Australian numbers)
    r'^\+1\(\d{3}\)\d{7}$',       # +1(514)9545771 (Canadian number format)
    r'^03[\u2013\u2014-]\d{8}$',  # 03–80917258 (Malaysian number with en-dash/em-dash/hyphen)
    r'^\+603-\d{8}\-ext\.\d{4,5}', # +603-8091 8000 ext 18208 (Malaysian landline with extension),
    r'^03-\d{8}\-ext\.\d{4,5}',   # 03-8091 8000 ext 18208 (Malaysian landline with extension without country code),
    r'^03-\d{8}/\d{3}',           # 03-22628400/442 (Malaysian landline with alt extension)
    r'^088-\d{6}/\d{3}',          # 088-xxxxxx/xxx (Sabah: Kota Kinabalu and Kudat numbers with extension seperated by '/')
    r'^03-\d{4}/\d{4}-ext\.\d{8}',# 03-xxxx/xxxx-ext.xxxxxxxx (Malaysian landline with variable digit used by KPT: Jabatan Pendidikan Politeknik & Kolej Komuniti)
    r'^\+06\d{7,8}',              # +06xxxxxxxx (Malaysian landline for Negeri Sembilan Malacca Muar, Johor Tangkak, Johor Batu Anam, Segamat and Johor)
    r'^\*?\d{4}',                 # *xxx (Extension for KPT)
    r'011-\d{8}',                 # 011-xxxxxxxx (Personal phone number)
    r'^1-300-\d{2}-\d{4}$',       # 1-300-xx-xxxx toll-free or service numbers
    r'^082-\d{6,10}$',           # '082-xxxxxxxxx' Sarawak 
    r'^088-\d{6,9}$',            # '088-xxxxxxxxx' Sabah 
    r'^\+41\d{9}$',                # Switzerland
    r'^\+44\d{10,11}$',            # UK 
    r'^\+1\d{10}$',              # Canadian
    r'^0[4-9]-\d{6,10}$',          # Pahang
    r'^0[4-9]-\d{6,8}/0[4-9]-\d{6,8}$'  # '09-7449223/09-7486645' or '05-2539529/05-2530526'

]

INVALID_PHONE_REGEX = [
    r'^03\-\d{3,5}$',               # 03-xxxxx (Malaysian landline with missing digits)
    r'^03\d{3,5}$',                 # 03xxxxx (Malaysian landline with missing digits)
    r'0{1,7}$',                    # 0000000 (Missing numbers from KPDN)
    r'1{3}$',                      # 111 (Missing numbers from KPDN)
    r'^03\s*$',  # Exactly '03' with optional whitespace # 03 (Landline area code for Selangor, KL Putrajaya, Genting, Pahang without number)
    r'^06\s*$',  # Exactly '06' with optional whitespace # 06 (Landline area code for Negeri Sembilan, Malacca, Muar (Johor) Tangkak (Johor) Batu Anam (Johor), Segamat (Johor) without number)
    r'^04-04\s*$',                     # 04-04 (Landline area code for Perlis, Kedah, Penang, Pengkalan Hulu (Perak) with partial number)
    r'03-\d{4}$',
    r'^03-\d{4}-$',              # '03-2771-'
    r'^\+60-\d{4}$'             # '+60-8871'
]

def load_json(file_path):
    """
    Loads and returns the JSON data from the provided file path.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

def save_json(data, file_name, output_folder):
    """
    Saves the processed data to the provided output folder.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    output_path = os.path.join(output_folder, file_name)
    with open(output_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)

    logging.info(f"Processed and saved to {output_path}")

    
def add_uuid(record):
    """
    Adds a new property 'id' with a generated UUID for the record.
    """
    record["id"] = str(uuid.uuid4())
    return record

def map_org_id_to_org_sort(record):
    """Map org_id to org_sort for each record using org_mapping.json"""
    org_mapping = load_org_mapping()  # oad org_mapping from file_utils.json

    if org_mapping is None:
        logging.error("Error: Could not load org_mapping.")
        return

    org_id = record.get('org_id')

    if not org_id:
        logging.warning(f"'org_id' missing in record: {record}")
        record['org_sort'] = 999999
        return

    if org_id in org_mapping:
        record['org_sort'] = org_mapping[org_id]
    else:
        logging.warning(f"'org_id' '{org_id}' not found in org_mapping.")
        record['org_sort'] = 999999

def validate_org_type(record):
    """Check the "org_type" field for validity"""
    org_type = record.get("org_type", "").lower()
    if org_type not in ALLOWED_ORG_TYPES:
        logging.warning(f"Invalid 'org_type' '{org_type}' in record: {record}")
    return record

def validate_person_email(record):
    """Validate the email format for the "person_email" field, including cleaning spaces"""
    email = record.get("person_email")
    if email and isinstance(email, str):
        email = email.replace(" ", "")
        record["person_email"] = email
        if not re.match(EMAIL_REGEX, email):
            logging.warning(f"Invalid 'person_email' '{email}' in record: {record}")
    return record

def validate_person_phone(record):
    """
    Validates the person_phone format based on the allowed patterns.
    Cleans up spaces and checks for valid patterns.
    """
    phone = record.get("person_phone")
    if phone and isinstance(phone, str):
    
        if phone in ["-", "0", "tiada", "."]:
            record["person_phone"] = None
            return record
        
        # Normalize phone number
        phone = phone.strip()
        phone = re.sub(r"^-\+", "+", phone)
        phone = re.sub(r'\([a-zA-Z ]*\)$|\((?=ext.)|(?<=\d{4})\)', " ", phone, flags=re.IGNORECASE) # Cleaning parentheses, keeps extension but removes text
        phone = re.sub(r'ext(\.\s{0,1}|\s*:\s*){0,1}', "-ext.", phone)
        phone = phone.replace("--", "-")
        phone = phone.replace("..", ".")
        phone = phone.replace(" ", "")
        phone = re.sub(r'^Telefon.*?(?=[06\(]|\+\d|-$)|-$', "", phone)
        phone = re.sub(r'\s+', "", phone)  # Removes all spaces, tabs, newlines inside the string
        phone = re.sub(r'\)(?!.*\()', "", phone)  # Remove misplaced closing parenthesis e.g "03-29358989-ext.205)" or "03-29358989-ext.205)/03-88836407"
        phone = re.sub(r'\((?!.*\))', "", phone)  # Remove misplaced opening parenthesis e.g '(+410227994044' or '09-5163251(128'

        # Return record if phone number is empty after normalization
        if not phone:
            record["person_phone"] = None
            return record

        # Add dash for numbers like '0362000591' -> '03-62000591'
        if re.match(r'^03\d{8}$', phone):  # If it's a 10-digit number starting with '03'
            phone = f'{phone[:2]}-{phone[2:]}'
        # Add dash for numbers like '60362000104' -> '603-62000104'
        elif re.match(r'^603\d{7,8}$', phone):  # If it's 7 or 8 digits starting with '603'
            phone = f'{phone[:3]}-{phone[3:]}'
        # Add dash for numbers like '+60362000104' -> '+603-62000104'
        elif re.match(r'^\+603\d{7,8}$', phone):  # If it's 7 or 8 digits starting with '+603'
            phone = f'{phone[:4]}-{phone[4:]}'
        # Add dash for numbers like '042625133-ext.101' -> '04-2625133-ext.101'
        elif re.match(r'^0\d{7,8}-ext\.\d+$', phone):
            phone = f'{phone[:2]}-{phone[2:]}'
        # Add dash for numbers like '042625133' -> '04-2625133'
        elif re.match(r'^0\d{7,8}$', phone):
            phone = f'{phone[:2]}-{phone[2:]}'
        # Add dash for numbers like '03-0388721983' -> '03-0388721983'
        elif re.match(r'^03-\d{9}$', phone):
            pass  # Number is already formatted correctly
        # Add dash and parentheses for numbers like '04-7314957(117)'
        elif re.match(r'^\d{2}-\d{7}\(\d{3}\)$', phone):
            pass  # Number is already formatted correctly
        # Add dash for numbers like '0313415437-ext.32845' -> '03-13415437-ext.32845
        elif re.match(r'^03\d{8}-ext.\d{4,5}', phone):
            phone = f"{phone[:2]}-{phone[2:]}"

        record["person_phone"] = phone
        
        # Check if the phone number matches any of the invalid patterns
        if any(re.match(pattern, phone, re.IGNORECASE) for pattern in INVALID_PHONE_REGEX):
            record["person_phone"] = None
        else:
            # Then, check if the phone number matches any valid patterns
            if not any(re.match(pattern, phone, re.IGNORECASE) for pattern in VALID_PHONE_REGEX):
                logging.warning(f"Invalid 'person_phone' '{phone}' in record: {record}")

    return record

def validate_required_keys(record):
    """
    Ensure that the given record contains all required keys and their values are of the correct data types.
    If a key is missing, has an incorrect data type, or is None when it shouldn't be, it will print a warning and attempt to fix it.
    """
    for key, meta in DATA_SCHEMA.items():
        if key not in record:
            logging.warning(f"'{key}' missing in record {record}, adding with default value None.")
            record[key] = None  # Set missing keys to None
        elif not isinstance(record[key], meta["type"]) and not (meta["nullable"] and record[key] is None):
            logging.warning(f"'{key}' has invalid data type. Expected {meta['type']}, got {type(record[key])}.")
            # Try to convert if possible, otherwise set to None
            try:
                record[key] = meta["type"](record[key])
            except (ValueError, TypeError):
                logging.warning(f"Unable to convert '{key}' to {meta['type']}, setting value to None.")
                record[key] = None

        # Check if the field must not be None (i.e., nullable is False)
        if record[key] is None and not meta["nullable"]:
            logging.warning(f"'{key}' should not be None, setting default value.")
            if meta["type"] == int:
                record[key] = 999999  # Set default for int
            else:
                record[key] = "TIADA"  # Set default for str

def strip_spaces(record):
    """Function to strip leading and trailing spaces from all string properties in a record"""
    for key, value in record.items():
        if isinstance(value, str):
            record[key] = value.strip()
    return record

def capitalize_values(record):
    """Function to capitalize values for specific keys"""
    for key in UPPERCASE_KEYS:
        if key in record and isinstance(record[key], str):
            record[key] = record[key].upper()
    
    for key in TITLECASE_KEYS:
        if key in record and isinstance(record[key], str):
            record[key] = record[key].title()
    
    return record

def standardize_position_sort_key(record):
    """
    Ensures the sorting key is standardized to 'position_sort_order'.
    If 'position_sort' is found, it is renamed to 'position_sort_order'.
    """
    if 'position_sort' in record:
        record['position_sort_order'] = record.pop('position_sort')
    return record

def sort_person_by_organisation(data):
    """Option 1: position_sort does not reset for each division (global sorting)"""
    # First standardize the keys
    data = [standardize_position_sort_key(record) for record in data]

    sorted_data = sorted(data, key=lambda x: (x['division_sort'], x['position_sort_order']))
    
    # Assign global position_sort
    for idx, record in enumerate(sorted_data):
        record.update({'position_sort': idx + 1})
    return sorted_data

def sort_person_by_division(data):
    """Option 2: position_sort resets for each division"""
    # First standardize the keys
    data = [standardize_position_sort_key(record) for record in data]
    
    # Sort by division_sort and position_sort_order
    sorted_data = sorted(data, key=lambda x: (x['division_sort'], x['position_sort_order']))
    
    current_division = None
    position_sort_counter = 0

    for record in sorted_data:
        #check if the division has changed and reset the position_sort_counter
        if record['division_sort'] != current_division:
            current_division = record['division_sort']
            position_sort_counter = 1  # Reset for a new division
        else:
            position_sort_counter += 1

        record.update({'position_sort': position_sort_counter})  # Set position_sort for the current division
    return sorted_data

def reorder_keys(record):
    """
    Reorders the keys in the record.
    """
    # Create a new ordered dictionary with the desired order
    new_record = {}

    # Add keys in the desired order
    new_record['org_id'] = record.get('org_id')
    new_record['org_name'] = record.get('org_name')
    new_record['org_sort'] = record.get('org_sort')
    new_record['org_type'] = record.get('org_type')
    new_record['division_name'] = record.get('division_name')
    new_record['division_sort'] = record.get('division_sort')
    new_record['subdivision_name'] = record.get('subdivision_name')
    new_record['position_sort'] = record.get('position_sort')
    new_record['position_name'] = record.get('position_name')
    new_record['person_name'] = record.get('person_name')
    new_record['person_email'] = record.get('person_email')
    new_record['person_fax'] = record.get('person_fax')
    new_record['person_phone'] = record.get('person_phone')
    new_record['parent_org_id'] = record.get('parent_org_id')

    return new_record

def remove_keys(record):
    """Function to remove specified keys from a single record"""
    keys_to_remove = ['position_sort_order']  # specify the keys to remove
    for key in keys_to_remove:
        if key in record:
            del record[key]
    return record

def sort_division_person(data, reset_per_division=True):
    """
    Combined function that lets you choose the sorting option.
    Sort records based on 'division_sort' and 'position_sort_order' or 'position_sort'. 
    If reset_per_division is True, position_sort is reset for each division.
    Otherwise, position_sort is globally assigned across the organisation.
    """
    if reset_per_division:
        return sort_person_by_division(data)  # Option 1: Reset position_sort for each division
    else:
        return sort_person_by_organisation(data)  # Option 2: Global position_sort by organisation

def data_processing_pipeline(data):
    """
    Processes all records in the given data list.
    Cleans, validates, and optionally sorts the records, then returns the processed data.
    """
    # Step 1: Process each record individually
    for record in data:
        validate_required_keys(record)
        strip_spaces(record)
        map_org_id_to_org_sort(record)
        validate_org_type(record)
        validate_person_email(record)
        validate_person_phone(record)
        capitalize_values(record)
        standardize_position_sort_key(record)

    # Step 2: Sort the data
    reset_per_division = True  # change to False if you want global sorting
    data = sort_division_person(data, reset_per_division)  # Comment out this line to skip sorting
    
    # Step 3: Remove unecessary key. Then, reorder the keys by creating a new data, after final processing and sorting.
    for idx, record in enumerate(data):
        remove_keys(record)
        reordered_record = reorder_keys(record)
        data[idx] = reordered_record  # Replace the original record with the reordered one
    
    return data

def process_json_file(json_file_path, output_folder):
    """Function to process a single JSON file and save the result"""
    json_file_name = os.path.basename(json_file_path)
    
    try:
        data = load_json(json_file_path)
        
        if isinstance(data, list):
            processed_data = data_processing_pipeline(data)
            
            save_json(processed_data, json_file_name, output_folder)
        else:
            logging.warning(f"Invalid JSON format in {json_file_name}, skipping file.")
            return
    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON in {json_file_name}. Skipping file.")
    except Exception as e:
        logging.error(f"An error occurred while processing {json_file_name}: {str(e)}")

def process_all_json_files(input_folder, output_folder):
    """
    Processes all JSON files in the input folder individually.
    Each file is processed using the pipeline and then saved to the output folder.
    """
    if not os.path.exists(input_folder):
        logging.error(f"The folder '{input_folder}' does not exist.")
        return

    for json_file in os.listdir(input_folder):
        if json_file.endswith('.json'):
            json_file_path = os.path.join(input_folder, json_file)
            process_json_file(json_file_path, output_folder)

if __name__ == "__main__":
    input_folder = 'data/input'
    output_folder = 'data/output'
    # input_folder = '../../data/input'
    # output_folder = '../../data/output'
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)  # Remove all files and subdirectories
        logging.info(f"Cleared the existing '{output_folder}' folder.")

    os.makedirs(output_folder)  # Recreate the empty folder
    logging.info(f"Created a new empty '{output_folder}' folder.")
    process_all_json_files(input_folder, output_folder)
