import os
import uuid
import json
import hashlib
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_CLEAN_DATA_FOLDER, INDEX_NAME, SHA_INDEX_NAME, ES_URL

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
ES_URL = ES_URL                  
INDEX_NAME = INDEX_NAME          
SHA_INDEX_NAME = SHA_INDEX_NAME  
DATA_FOLDER = os.path.join(BASE_DIR, DEFAULT_CLEAN_DATA_FOLDER)

COLUMNS_TO_HASH = [
    "org_sort", "org_id", "org_name", "org_type", "division_sort",
    "division_name", "subdivision_name", "position_sort", "position_name",
    "person_name", "person_email", "person_phone", "person_fax",
    "parent_org_id"
]

mapping = {
    "properties": {
        "org_id": {"type": "keyword"},
        "org_name": {
            "type": "search_as_you_type",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "org_sort": {"type": "integer"},
        "org_type": {"type": "keyword"},
        "division_name": {
            "type": "search_as_you_type",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "division_sort": {"type": "integer"},
        "subdivision_name": {
            "type": "search_as_you_type",
            "fields": {
                "keyword": {
                    "type": "keyword"
                }
            }
        },
        "position_name": {"type": "search_as_you_type"},
        "person_name": {"type": "search_as_you_type"},
        "person_email": {"type": "keyword", "null_value": "NULL"},
        "person_fax": {"type": "keyword", "null_value": "NULL"},
        "person_phone": {"type": "keyword", "null_value": "NULL"},
        "position_sort": {"type": "integer"},
        "parent_org_id": {"type": "keyword", "null_value": "NULL"},
    }
}

es = Elasticsearch(ES_URL)

def calculate_sha256_for_document(doc):
    """
    Calculate SHA-256 hash based on specific columns of a document.
    """
    filtered_doc = {col: doc.get(col, "") for col in COLUMNS_TO_HASH if col in doc}
    doc_str = json.dumps(filtered_doc, sort_keys=True)
    return hashlib.sha256(doc_str.encode("utf-8")).hexdigest()

def calculate_sha256_for_file(file_path):
    """
    Calculate SHA-256 hash for the entire file content.
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

def check_and_update_file_sha(file_path):
    new_sha = calculate_sha256_for_file(file_path)
    response = es.options(ignore_status=404).get(index=SHA_INDEX_NAME, id=os.path.basename(file_path))
    stored_sha = response["_source"]["sha"] if response.get("found") else None

    task_id = f"{os.path.basename(file_path)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if new_sha != stored_sha:
        # Store the updated SHA in Elasticsearch
        es.index(index=SHA_INDEX_NAME, id=os.path.basename(file_path), document={"sha": new_sha, "task_id": task_id})
        return True  # Data has changed
    return False  # No changes

def check_sha_and_update(data_folder):
    """
    Check if there are any changes in the files' SHA-256 hashes.
    Returns a list of files with changed SHAs.
    """
    changed_files = []
    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)
        if file_name.endswith(".json"):
            if check_and_update_file_sha(file_path):
                print(f"STATUS {file_name}: Changes detected.")
                changed_files.append(file_path)
            else:
                print(f"STATUS {file_name}: No changes.")
    return changed_files

def delete_documents_by_org_id(org_id):
    """Delete all documents in Elasticsearch with the specified org_id."""
    delete_query = {
        "query": {
            "term": {
                "org_id": org_id
            }
        }
    }
    try:
        es.delete_by_query(index=INDEX_NAME, body=delete_query)
        print(f"Deleted existing documents with org_id : {org_id} from Elasticsearch.")
    except Exception as e:
        print(f"Error deleting documents with org_id {org_id}: {e}")

def upload_clean_data_to_es(files_to_upload):
    """Upload JSON documents to Elasticsearch only for the specified files."""
    for file_path in files_to_upload:
        file_name = os.path.basename(file_path)
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        if data:
            # Retrieve the org_id from the first document (assuming all documents share the same org_id)
            org_id = data[0].get("org_id")
            if org_id:
                # Delete existing documents for this org_id before re-indexing
                delete_documents_by_org_id(org_id)
            
        actions = []
        for doc in data:
            sha_256_hash = calculate_sha256_for_document(doc)  # Calculate SHA for change tracking
            doc['sha_256_hash'] = sha_256_hash  # Store SHA in the document
            
            # Generate a new UUID for each document's Elasticsearch _id
            doc_id = str(uuid.uuid4())
            actions.append({
                "_index": INDEX_NAME,
                "_id": doc_id,
                "_source": doc
            })

        if actions:
            print(f"Indexing {len(actions)} documents from {file_name} to Elasticsearch...")
            success, failed = bulk(es, actions)
            print(f"\nSuccessfully indexed {success} documents.")
            if failed:
                print("\nSome documents failed:", failed)

def get_elasticsearch_info():
    """Get Elasticsearch cluster info for debugging connection issues."""
    try:
        resp = es.info()
        print("Elasticsearch Info:")
        print(json.dumps(resp.body, indent=2))
        return True
    except Exception as e:
        print(f"Error getting Elasticsearch info: {e}")
        return False

def create_index_if_not_exists():
    """Create the Elasticsearch index if it does not exist."""
    try:
        if es.indices.exists(index=INDEX_NAME):
            print(f'Index "{INDEX_NAME}" already exists.')
        else:
            es.indices.create(index=INDEX_NAME, body={"mappings": mapping})
            print(f'Index "{INDEX_NAME}" created with the provided mapping.')
        return True
    except Exception as e:
        print(f"Error creating or checking index: {e}")
        return False

def main():
    """Main function to check for changes and upload data to Elasticsearch."""
    if not get_elasticsearch_info():
        print("Skipping indexing due to Elasticsearch connection issues.")
        return
        
    if create_index_if_not_exists():
        changed_files = check_sha_and_update(DATA_FOLDER)
        if changed_files:
            upload_clean_data_to_es(changed_files)
        else:
            print("\nNo changes detected in data. Skipping upload.")
    else:
        print("Skipping indexing due to index creation issues.")

if __name__ == "__main__":
    main()
