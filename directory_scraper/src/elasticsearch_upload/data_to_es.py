import os
import json
import hashlib
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from datetime import datetime
import sys
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
    """
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            sha256.update(chunk)
    return sha256.hexdigest()

def check_and_update_file_sha(file_path):
    new_sha = calculate_sha256_for_file(file_path)
    response = es.options(ignore_status=404).get(index=ES_SHA_INDEX, id=os.path.basename(file_path))
    stored_sha = response["_source"]["sha"] if response.get("found") else None

    task_id = f"{os.path.basename(file_path)}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if new_sha != stored_sha:
        # Store the updated SHA in Elasticsearch
        es.index(index=ES_SHA_INDEX, id=os.path.basename(file_path), document={"sha": new_sha, "task_id": task_id})
        print(f'Status index "{ES_SHA_INDEX}" | {os.path.basename(file_path)}: UPDATED')
        return True  # Data has changed
    else:
        print(f'Status index "{ES_SHA_INDEX}" | {os.path.basename(file_path)}: NO CHANGES')
    return False  # No changes

def check_sha_and_update(data_folder):
    """
    Check if there are any changes in the files' SHA-256 hashes.
    Returns a list of files with changed SHAs.
    """    
    changed_files = []
    for file_name in os.listdir(data_folder):
        file_path = os.path.join(data_folder, file_name)
        if file_name.endswith(".json") and check_and_update_file_sha(file_path):
            print(f'Status file | {file_name}: CHANGES DETECTED')
            changed_files.append(file_path)
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

def upload_clean_data_to_es(files_to_upload):
    """Upload JSON documents to Elasticsearch, using new files as the source of truth."""
    all_summaries = [] 
    changes_log = {}

    for file_path in files_to_upload:
        file_name = os.path.basename(file_path)
        
        with open(file_path, 'r') as f:
            new_data = json.load(f)

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

            # Initialize change tracking for this org_id
            changes_log[org_id] = {"added": [], "updated": [], "deleted": []}

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
                        print(f"Updating document: {document_id}")
                        actions.append({
                            "_index": ES_INDEX,
                            "_id": document_id,
                            "_source": doc
                        })
                        changes_log[org_id]["updated"].append({
                            "_id": document_id,
                            "before": existing_doc,
                            "after": doc
                        })
                        updated_count += 1
                else:
                    # Add new document
                    print(f"Adding new document: {document_id}")
                    actions.append({
                        "_index": ES_INDEX,
                        "_id": document_id,
                        "_source": doc
                    })
                    changes_log[org_id]["added"].append({
                        "_id": document_id,
                        "doc": doc
                    })
                    added_count += 1

            # Identify and delete stale documents (those in Elasticsearch but not in new_data)
            stale_docs = set(existing_docs_by_id.keys()) - processed_ids
            for stale_id in stale_docs:
                print(f"Deleting stale document: {stale_id}")
                actions.append({
                    "_op_type": "delete",
                    "_index": ES_INDEX,
                    "_id": stale_id
                })
                changes_log[org_id]["deleted"].append({
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

def main(data_folder=None):
    """Main function to check for changes and upload data to Elasticsearch."""
    data_folder = data_folder or DATA_FOLDER

    if not get_elasticsearch_info():
        print("Skipping indexing due to Elasticsearch connection issues.")
        if DISCORD_WEBHOOK_URL:
            send_discord_notification(f"Skipping indexing due to Elasticsearch connection issues.", DISCORD_WEBHOOK_URL, THREAD_ID)
        else:
            print("Discord webhook URL not provided. Skipping notifications.") 
        return
        
    if create_index_if_not_exists():
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