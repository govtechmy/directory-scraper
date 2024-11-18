import json
import logging
import elasticsearch as es
from elasticsearch.helpers import bulk
from elasticsearch_upload.script import DATA_FILE, INDEX_NAME, get_elasticsearch_info, create_index_if_not_exists

logger = logging.getLogger(__name__)
logging.basicConfig(filename="gsheet_api.log", filemode="a", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

        logging.info(f"Sending bulk request to Elasticsearch for {len(data)} documents...")
        success, failed = bulk(es, generate_actions())

        logging.info(f"Bulk indexing completed. Success: {success}, Failed: {len(failed)}")
        if failed:
            logging.error("Failed actions:")
            for item in failed:
                logging.error(json.dumps(item, indent=2))

        count = es.count(index=INDEX_NAME)['count']
        logging.info(f"Number of documents in index: {count}")
    except Exception as e:
        logging.error(f"Error during indexing: {e}")
        logging.error(f"Error type: {type(e)}")
        logging.error(f"Error details: {str(e)}")

def main():
    try:
        if get_elasticsearch_info():
            if create_index_if_not_exists():
                index_json_file()
            else:
                logging.warning("Skipping indexing due to index creation issues.")
        else:
            logging.error("Skipping indexing due to Elasticsearch connection issues.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        logging.error(f"Error type: {type(e)}")
        logging.error(f"Error details: {str(e)}")
    finally:
        es.close()