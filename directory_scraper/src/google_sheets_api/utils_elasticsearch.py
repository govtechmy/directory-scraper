import json
import elasticsearch as es
from elasticsearch.helpers import bulk
from elasticsearch_upload.script import DATA_FILE, INDEX_NAME, get_elasticsearch_info, create_index_if_not_exists

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