import os
import sys
import json
import hashlib
from io import BytesIO
from datetime import datetime

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
from directory_scraper.path_config import DEFAULT_CLEAN_DATA_FOLDER
from directory_scraper.src.utils.discord_bot import send_discord_notification

load_dotenv()

DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL') 
THREAD_ID = os.getenv('THREAD_ID')

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
ES_URL = os.getenv('ES_URL') #ES_URL
ES_INDEX = os.getenv('ES_INDEX')
ES_LOG_INDEX = os.getenv('ES_LOG_INDEX')
ES_SHA_INDEX = os.getenv('ES_SHA_INDEX')
ES_API_KEY = os.getenv('ES_API_KEY') #""
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
        "sha_256_hash": {"type": "keyword"},
    }
}

logs_mapping = {
    "properties": {
        "sheet_id": {"type": "keyword"},
        "person_email": {"type": "keyword", "null_value": "NULL"},
        "sha_256_hash": {"type": "keyword"},
        "@timestamp": {
            "type": "date",
            "format": "yyyy-MM-dd HH:mm:ss"
        }
    }
}

api_key_info = ES_API_KEY
es = Elasticsearch(
    ES_URL,
    api_key=api_key_info if api_key_info else None,
    timeout=30,
    max_retries=3,
    retry_on_timeout=True
)

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
    Handles on-disk files and in-memory files differently.
    """
    sha256 = hashlib.sha256()
    if isinstance(file_path, (str,)):
        f = open(file_path, "rb")

    elif isinstance(file_path, (list,)):
        data_txt = "[\n" + ",\n".join([json.dumps(row, separators=(",", ":"), ensure_ascii=False) for row in file_path]) + "\n]"
        f = BytesIO(data_txt.encode("utf-8"))

    try:
        while chunk := f.read(8192):
            sha256.update(chunk)
    except Exception as e:
        raise e
    finally:
        f.close()

    return sha256.hexdigest()

def check_and_update_file_sha(file_path):
    """
    Calculate SHA-256 for a file and compares it to the version in ElasticSearch.
    Handles on-disk files and in-memory files differently.
    """
    if isinstance(file_path, (str,)):
        file_id = os.path.basename(file_path)
    elif isinstance(file_path, (dict,)):
        file_id = file_path.get("file_name", None)

    new_sha = calculate_sha256_for_file(file_path)
    response = es.options(ignore_status=404).get(index=ES_SHA_INDEX, id=file_id)
    stored_sha = response["_source"]["sha"] if response.get("found") else None

    task_id = f"{file_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if new_sha != stored_sha:
        # Store the updated SHA in Elasticsearch
        es.index(index=ES_SHA_INDEX, id=file_id, document={"sha": new_sha, "task_id": task_id})
        print(f'Status index "{ES_SHA_INDEX}" | {file_id}: UPDATED')
        return True  # Data has changed
    else:
        print(f'Status index "{ES_SHA_INDEX}" | {file_id}: NO CHANGES')
    return False  # No changes

def check_sha_and_update(data_folder):
    """
    Check if there are any changes in the files' SHA-256 hashes.
    Returns a list of files with changed SHAs.
    Handles on-disk files and in-memory files differently.
    """    
    changed_files = []
    if isinstance(data_folder, (str,)):
        for file_name in os.listdir(data_folder):
            file_path = os.path.join(data_folder, file_name)
            if file_name.endswith(".json") and check_and_update_file_sha(file_path):
                print(f'Status file | {file_name}: CHANGES DETECTED')
                changed_files.append(file_path)

    elif isinstance(data_folder, (list,)):
        for file in data_folder:
            file_name = file.get("file_name", None)
            file_data = file.get("file_data", None)
            if file_name and file_data and check_and_update_file_sha(file):
                changed_files.append(file)

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
        es.delete_by_query(index=ES_INDEX, body=delete_query)
        print(f"Deleted existing documents with org_id : {org_id}")
    except Exception as e:
        print(f"Error deleting documents with org_id {org_id}: {e}")

def upload_clean_data_to_es(files_to_upload, log_changes=True):
    """Upload JSON documents to Elasticsearch, using new files as the source of truth."""
    all_summaries = [] 
    changes_log = {
        "metadata": {
            "timestamp": datetime.now().isoformat() + "Z", 
            "description": "Logs of data changes uploaded to Elasticsearch",
            "files_processed": files_to_upload
        },
        "changes": {}
    } if log_changes else None

    for file_path in files_to_upload:
        if isinstance(files_to_upload, (str,)):
            file_name = os.path.basename(file_path)
            
            with open(file_path, 'r') as f:
                new_data = json.load(f)

        elif isinstance(files_to_upload, (dict,)):
            file_name = file_path.get("file_name", None)
            new_data = file_path.get("file_data", None)
            if not file_name or not new_data:
                raise ValueError("Warning - files_to_upload is neither a path or a dictionary. Check the input and rerun the function")

        # Group documents by org_id
        org_id_groups = {}
        for doc in new_data:
            org_id = doc.get("org_id")
            if org_id not in org_id_groups:
                org_id_groups[org_id] = []
            org_id_groups[org_id].append(doc)

        # Process each org_id group separately
        for org_id, docs in org_id_groups.items():
            print(f"\nChecking org_id: {org_id} in file: {file_name}")

            if log_changes:
                changes_log["changes"][org_id] = {"added": [], "updated": [], "deleted": []}

            # Load existing documents for this org_id
            existing_docs_query = {"query": {"term": {"org_id": org_id}}, "size": 10000}
            existing_docs = es.search(index=ES_INDEX, body=existing_docs_query)
            existing_docs_by_id = {hit['_id']: hit['_source'] for hit in existing_docs['hits']['hits']}
            
            actions = []
            processed_ids = set()

            # Initialize counters for added, updated, and deleted documents
            added_count = 0
            updated_count = 0
            deleted_count = 0

            for doc in docs:
                sha_256_hash = calculate_sha256_for_document(doc)
                doc['sha_256_hash'] = sha_256_hash
                document_id = f"{doc.get('org_id', '')}_{str(doc.get('division_sort', '')).zfill(3)}_{str(doc.get('position_sort', '')).zfill(6)}"

                # Mark this document as processed, even if unchanged
                processed_ids.add(document_id)

                if document_id in existing_docs_by_id:
                    # Update if SHA differs
                    existing_doc = existing_docs_by_id[document_id]
                    if existing_doc['sha_256_hash'] != sha_256_hash:
                        # print(f"Updating document: {document_id}")
                        actions.append({
                            "_index": ES_INDEX,
                            "_id": document_id,
                            "_source": doc
                        })
                        if log_changes:
                            changes_log["changes"][org_id]["updated"].append({
                                "_id": document_id,
                                "before": existing_doc,
                                "after": doc
                            })
                        updated_count += 1
                else:
                    # Add new document
                    # print(f"Adding new document: {document_id}")
                    actions.append({
                        "_index": ES_INDEX,
                        "_id": document_id,
                        "_source": doc
                    })
                    if log_changes:
                        changes_log["changes"][org_id]["added"].append({
                            "_id": document_id,
                            "doc": doc
                        })
                    added_count += 1

            # Identify and delete stale documents (those in Elasticsearch but not in new_data)
            stale_docs = set(existing_docs_by_id.keys()) - processed_ids
            for stale_id in stale_docs:
                # print(f"Deleting stale document: {stale_id}")
                actions.append({
                    "_op_type": "delete",
                    "_index": ES_INDEX,
                    "_id": stale_id
                })
                if log_changes:
                    changes_log["changes"][org_id]["deleted"].append({
                        "_id": stale_id,
                        "doc": existing_docs_by_id[stale_id]
                    })
                deleted_count += 1

            # Execute bulk actions for this org_id
            if actions:
                print(f"Processing {len(actions)} actions for org_id {org_id}...")
                success, failed = bulk(es, actions)
                print(f"Successfully processed {success} actions.")
                if failed:
                    print("\nSome actions failed:", failed)

            # Print summary for each org_id in the file
            print(f"Summary for org_id {org_id}:")
            print(f"  - Added: {added_count}")
            print(f"  - Updated: {updated_count}")
            print(f"  - Deleted: {deleted_count}")

            summary = (f"🛢️ {org_id} - Added: {added_count}, Updated: {updated_count}, Deleted: {deleted_count}")
            all_summaries.append(summary)

    # Save the changes log to a JSON file
    if log_changes and changes_log:
        with open("es_changes_log.json", "w") as log_file:
            json.dump(changes_log, log_file, indent=4)

    return all_summaries


def get_elasticsearch_info():
    """Get Elasticsearch cluster info for debugging connection issues."""
    try:
        resp = es.info()
        #print("Elasticsearch Info:")
        #print(json.dumps(resp.body, indent=2))
        return True
    except Exception as e:
        print(f"Error getting Elasticsearch info: {e}")
        return False

def create_index_if_not_exists():
    """Create the Elasticsearch index if it does not exist."""
    try:
        if es.indices.exists(index=ES_INDEX):
            print(f'Index "{ES_INDEX}" already exists.')
        else:
            es.indices.create(index=ES_INDEX, body={"mappings": mapping})
            print(f'Index "{ES_INDEX}" created with the provided mapping.')
        return True
    except Exception as e:
        print(f"Error creating or checking index: {e}")
        return False

def create_logs_if_not_exists():
    """Create the Elasticsearch edit logs index if it does not exist."""
    try:
        if es.indices.exists(index=ES_LOG_INDEX):
            print(f'Index "{ES_LOG_INDEX}" already exists.')
        else:
            es.indices.create(index=ES_LOG_INDEX, body={"mappings": logs_mapping})
            print(f'Index "{ES_LOG_INDEX}" created with the provided mapping.')
        return True            
    except Exception as e:
        print(f"Error creating or checking index: {e}")
        return False

def main(data_folder=None):
    """
    Main function to check for changes and upload data to Elasticsearch.
    `data_folder` input should either be a path to the folder containing
    json files to be uploaded or a list of dictionaries of the data.
    """
    if isinstance(data_folder, (str,)):
        data_folder = data_folder or DATA_FOLDER

    if not get_elasticsearch_info():
        print("Skipping indexing due to Elasticsearch connection issues.")
        if DISCORD_WEBHOOK_URL:
            send_discord_notification(f"Skipping indexing due to Elasticsearch connection issues.", DISCORD_WEBHOOK_URL, THREAD_ID)
        else:
            print("Discord webhook URL not provided. Skipping notifications.") 
        return
        
    if create_index_if_not_exists() and create_logs_if_not_exists():
        changed_files = check_sha_and_update(data_folder)
        if changed_files:
            all_summaries = upload_clean_data_to_es(changed_files)
            # Consolidate and send a single Discord notification
            if all_summaries:
                final_summary_message = "\n".join(all_summaries)
                print("Final Summary:\n", final_summary_message)
                if DISCORD_WEBHOOK_URL:
                    send_discord_notification(final_summary_message, DISCORD_WEBHOOK_URL, THREAD_ID)
                else:
                    print("Discord webhook URL not provided. Skipping notifications.")
        else:
            print("\nNo changes detected in data. Skipping upload.")
            if DISCORD_WEBHOOK_URL:
                send_discord_notification(f"🛢️ No changes detected in data. Skipping upload to ES.", DISCORD_WEBHOOK_URL, THREAD_ID)
    else:
        print("Skipping indexing due to index creation issues.")

if __name__ == "__main__":
    main()