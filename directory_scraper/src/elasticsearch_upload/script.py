import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Elasticsearch configuration from .env
ES_URL = os.getenv('ES_URL')
INDEX_NAME = os.getenv('INDEX_NAME')
API_KEY_FILE = os.getenv('API_KEY_FILE')
DATA_FILE = os.getenv('DATA_FILE')

# Updated mapping to match the new data structure
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
        "unit_name": {
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

def read_api_key():
    try:
        with open(API_KEY_FILE, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error reading API key file: {e}")
        sys.exit(1)

# Initialize Elasticsearch client
api_key_info = read_api_key()
es = Elasticsearch(
    ES_URL,
    api_key=(api_key_info['id'], api_key_info['api_key'])
)

def get_elasticsearch_info():
    try:
        resp = es.info()
        print("Elasticsearch Info:")
        print(json.dumps(resp.body, indent=2))
        return True
    except Exception as e:
        print(f"Error getting Elasticsearch info: {e}")
        print(f"Error type: {type(e)}")
        print(f"Error details: {str(e)}")
        return False

def create_index_if_not_exists():
    try:
        if not es.indices.exists(index=INDEX_NAME):
            es.indices.create(index=INDEX_NAME, body={"mappings": mapping})
            print(f'Index "{INDEX_NAME}" created with mapping.')
        else:
            print(f'Index "{INDEX_NAME}" already exists.')
        return True
    except Exception as e:
        print(f"Error creating index: {e}")
        print(f"Error type: {type(e)}")
        print(f"Error details: {str(e)}")
        return False

def index_json_file():
    try:
        with open(DATA_FILE, 'r') as f:
            data = json.load(f)

        def generate_actions():
            for doc in data:
                yield {
                    "_index": INDEX_NAME,
                    "_id": f"{doc.get('org_id', '')}_{str(doc.get('division_sort', '')).zfill(3)}_{str(doc.get('position_sort', '')).zfill(6)}",
                    "_source": doc
                }

        print(f"Sending bulk request to Elasticsearch for {len(data)} documents...")
        success, failed = bulk(es, generate_actions())

        print(f"Bulk indexing completed. Success: {success}, Failed: {len(failed)}")
        if failed:
            print("Failed actions:")
            for item in failed:
                print(json.dumps(item, indent=2))

        count = es.count(index=INDEX_NAME)['count']
        print(f"Number of documents in index: {count}")
    except Exception as e:
        print(f"Error during indexing: {e}")
        print(f"Error type: {type(e)}")
        print(f"Error details: {str(e)}")

def main():
    try:
        if get_elasticsearch_info():
            if create_index_if_not_exists():
                index_json_file()
            else:
                print("Skipping indexing due to index creation issues.")
        else:
            print("Skipping indexing due to Elasticsearch connection issues.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        print(f"Error type: {type(e)}")
        print(f"Error details: {str(e)}")
    finally:
        es.close()

if __name__ == "__main__":
    main()