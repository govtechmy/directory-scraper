# WARNING: This file is deprecated, to maintain elasticsearch functions for API backend, take functions from elasticsearch_upload module
import json
import logging
import hashlib
from io import BytesIO
from typing import List
import elasticsearch as es
from datetime import datetime
from elasticsearch.helpers import bulk
from elasticsearch_upload.data_to_es import (
    get_elasticsearch_info, create_index_if_not_exists, upload_clean_data_to_es,
    ES_SHA_INDEX
)

logger = logging.getLogger(__name__)
logging.basicConfig(filename="gsheet_api.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(pathname)s - Line %(lineno)d - %(levelname)s - %(message)s')


def inmemory_calculate_sha256_for_file(file_data: List[dict]):
    """
    Calculate SHA-256 hash for the entire file content.
    file_data input must be in 'records' format
    """
    data_txt = "[\n" + ",\n".join([json.dumps(row, separators=(",", ":"), ensure_ascii=False) for row in file_data]) + "\n]"
    with BytesIO(data_txt.encode("utf-8")) as file_buf:
        sha256 = hashlib.sha256()
        while chunk := file_buf.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()


def inmemory_check_and_update_file_sha(file_name: str, file_data: List[dict]):
    """
    Similary to check_and_update_file_sha, but everything is done in-memory.
    Computes and compares file_data hash against Elasticsearch.
    Returns boolean if data has changed or not.
    """
    new_sha = inmemory_calculate_sha256_for_file(file_data)
    response = es.options(ignore_status=404).get(index=ES_SHA_INDEX, id=file_name)
    stored_sha = response["_source"]["sha"] if response.get("found") else None

    task_id = f"{file_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if new_sha != stored_sha:
        # Store the updated SHA in Elasticsearch
        es.index(index=ES_SHA_INDEX, id=file_name, document={"sha": new_sha, "task_id": task_id})
        logging.info(f'Status index "{ES_SHA_INDEX}" | {file_name}: UPDATED')
        return True  # Data has changed
    else:
        print(f'Status index "{ES_SHA_INDEX}" | {file_name}: NO CHANGES')
    return False  # No changes


def inmemory_check_sha_and_update(data_lst: List[dict]):
    """ 
    Similar to check_sha_and_update function, but everything is done in-memory.
    Check if there are any changes in the files' SHA-256 hashes.
    Returns a list of files with changed SHAs.
    """    
    changed_files = []
    for file in data_lst:
        file_name = file.get("file_name", None)
        file_data = file.get("data", None)
        if not file_name and not file_data and inmemory_check_and_update_file_sha(file_name, file_data):
            changed_files.append(file)
    return changed_files


def push_to_es(data_lst=None):
    """
    Main function to check for changes and upload data to Elasticsearch.
    Adapted to work with in-memory files.
    """

    if not get_elasticsearch_info():
        print("Skipping indexing due to Elasticsearch connection issues.")
        return
        
    if create_index_if_not_exists():
        changed_files = inmemory_check_sha_and_update(data_lst)
        if changed_files:
            upload_clean_data_to_es(changed_files)
        else:
            print("\nNo changes detected in data. Skipping upload.")
    else:
        print("Skipping indexing due to index creation issues.")


if __name__ == "__main__":
    push_to_es()