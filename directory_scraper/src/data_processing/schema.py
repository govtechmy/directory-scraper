"""
This schema.py is expected to be updated if new data category is introduced.

Each data category corresponds to a specific folder under spiders/ (e.g., ministry, bahagian_unit) and has a unique schema and processing logic. 
This script ensures seamless integration of these data categories into the relevant processing pipeline.

********************* Adding a New Data Category *********************
To add support for a new data category, follow these steps:

1. Create a Utility Script (if needed):
- Create a new utility script/functions (if needed) in the utils/ folder for the new category.
- The utility script should contain the data validation, transformation, and cleaning functions specific to that category.
file: utils_process.py

2. Define a Processor Class:
- Create a new processor class in schema.py that extends BaseProcessor.
- Implement two required methods in the class:
    -> process_record(record): Processes a single record (e.g., validate fields, transform data).
    -> process_pipeline(data): Processes an entire dataset (e.g., apply sorting, remove invalid records).

3. Update the Schema Mapping and Definitions:
- Add a new schema definition in the SCHEMA_DEFINITIONS dictionary for the new category:
'''
SCHEMA_DEFINITIONS = {
    ...
    "NewCategory": {
        "key_name": {"type": str, "nullable": False},
        ...
    },
}
'''

- **Key Rules for Schema Definitions**:
    - `type`: Specifies the expected data type (e.g., `str`, `int`, etc.).
    - `nullable`: Indicates whether the field can be `None`.
        - If `False`, missing or invalid fields will be set to default values:
            - `""` for `str`, `0` for `int`, or as defined in the code.
    - Add meaningful keys to ensure the output aligns with the schema.

- Add an entry in the SCHEMA_MAPPING dictionary:
'''
SCHEMA_MAPPING = {
    ...
    "new_category_folder": "NewProcessorName",
}
'''
- new_category_folder: Name of the folder under spiders/ corresponding to the new data category.
- NewProcessorName: The name of the processor class you defined in Step 2.

4. Update the Processor Registry:
- Add an entry in the SCHEMA_REGISTRY dictionary
'''
SCHEMA_REGISTRY = {
    ...
    "NewProcessorName": NewProcessorClass,
}
'''
- NewProcessorName: Must match the name in SCHEMA_MAPPING.
- NewProcessorClass: The class you defined in Step 2.

5. Test the Integration:

- Ensure the new category works as expected by running the entire pipeline with test data.
- Validate outputs in the relevant output folder under data/output/

************************End of Adding a New Data Category*********************


"""
import logging

from directory_scraper.src.data_processing.utils.utils_process import (
    validate_person_name,
    validate_org_type,
    validate_person_email,
    validate_person_phone,
    map_org_id_to_org_sort,
    strip_spaces,
    capitalize_values,
    sort_division_person,
    reorder_keys,
    remove_keys,
)


# ============================== SCHEMA DEFINITIONS ==============================

DIRECTORY_SCHEMA = {
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
 
BAHAGIAN_SCHEMA = { #EXAMPLE
    "org_id": {"type": str, "nullable": False},
    "division_name": {"type": str, "nullable": False},
    "subdivision_name": {"type": str, "nullable": True},
    "function_description": {"type": str, "nullable": True},
}

SCHEMA_DEFINITIONS = {
    "DIRECTORY": DIRECTORY_SCHEMA,
    "BAHAGIAN": BAHAGIAN_SCHEMA,
}

# ============================== END OF SCHEMA DEFINITIONS ==============================

# ============================== VALIDATION FUNCTIONS ==============================

def validate_required_keys(record, schema_name):
    """
    Validates a record against the schema specified by schema_name.
    Ensures all required keys are present and match their data types.

    Args:
        record (dict): The record to validate.
        schema_name (str): The schema name to validate against.

    Raises:
        ValueError: If a required key is missing or has an invalid data type.
    """
    # Fetch the appropriate schema from SCHEMA_DEFINITIONS
    schema = SCHEMA_DEFINITIONS.get(schema_name)
    if not schema:
        raise ValueError(f"Schema '{schema_name}' not found.")

    for key, meta in schema.items():
        # Check if the key is present in the record
        if key not in record:
            logging.warning(f"'{key}' missing in record {record}, adding with default value None.")
            record[key] = None

        # Validate type or ensure nullable fields can be None
        elif not isinstance(record[key], meta["type"]) and not (meta["nullable"] and record[key] is None):
            logging.warning(f"'{key}' has invalid data type. Expected {meta['type']}, got {type(record[key])} from record: {record}")
            try:
                record[key] = meta["type"](record[key])
            except (ValueError, TypeError):
                logging.warning(f"Unable to convert '{key}' to {meta['type']}, setting to None.")
                record[key] = None

        # Check if non-nullable fields are None and set defaults
        if record[key] is None and not meta["nullable"]:
            logging.warning(f"'{key}' must not be None. Setting default value.")
            if meta["type"] == int:
                record[key] = 999999  # Default for integers
            elif meta["type"] == str:
                record[key] = "TIADA"  # Default for strings
            else:
                record[key] = meta["type"]()  # Generic default for other types

# ============================== END OF VALIDATION FUNCTIONS ==============================

# ============================== PROCESSORS ==============================

def get_processor(schema_name):
    """
    Get the processor explicitly based on the schema name.
    """
    processor = SCHEMA_REGISTRY.get(schema_name)
    if not processor:
        raise ValueError(f"Schema '{schema_name}' not found in registry.")
    return processor

class BaseProcessor:
    """
    Base class for data processors.
    """
    def process_record(self, record):
        raise NotImplementedError

    def process_pipeline(self, data):
        raise NotImplementedError

# ============================== END OF PROCESSORS ==============================

# ============================== PROCESSORS CLASS ==============================

class DirectoryProcessor(BaseProcessor):
    """
    Processor for Directory data schema.
    """
    SCHEMA_NAME = "DIRECTORY"

    def process_record(self, record):
        validate_required_keys(record, self.SCHEMA_NAME)
        validate_person_name(record)
        validate_org_type(record)
        validate_person_email(record)
        validate_person_phone(record)
        map_org_id_to_org_sort(record)
        strip_spaces(record)
        capitalize_values(record)
        return record

    def process_pipeline(self, data):
        """
        The full processing pipeline for Directory data.
        """
        # Step 1: Process each record
        faulty_record_index = []
        for idx, record in enumerate(data):
            try:
                data[idx] = self.process_record(record)
            except ValueError as err:
                faulty_record_index.append(idx)
                logging.warning(str(err))
                continue

        # Remove invalid records
        if faulty_record_index:
            for idx in reversed(faulty_record_index):
                data.pop(idx)

        # Step 2: Sort the data
        data = sort_division_person(data, reset_per_division=True)

        # Step 3: Reorder keys and remove unnecessary keys
        for idx, record in enumerate(data):
            record = remove_keys(record)
            data[idx] = reorder_keys(record)

        return data

class BahagianProcessor(BaseProcessor):
    """
    Processor for Bahagian data schema.
    """
    SCHEMA_NAME = "BAHAGIAN"

    def process_record(self, record):
        validate_required_keys(record, self.SCHEMA_NAME)
        strip_spaces(record)
        return record

    def process_pipeline(self, data):
        """
        The full processing pipeline for Bahagian data.
        """
        processed_data = [self.process_record(record) for record in data]
        return processed_data

# ============================== END OF PROCESSORS CLASS ==============================

#=============================== SCHEMA MAPPING ==============================

SCHEMA_MAPPING = {
    "ministry": "Directory",
    "non_ministry": "Directory",
    "bahagian_unit": "FungsiBahagian",
}

SCHEMA_REGISTRY = {
    "Directory": DirectoryProcessor,
    "FungsiBahagian": BahagianProcessor,
}

#=============================== END OF SCHEMA MAPPING ========================

