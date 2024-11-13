import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))  # points to the main project directory

# Data processing
DEFAULT_SPIDERS_OUTPUT_FOLDER = "data/spiders_output"
DEFAULT_CLEAN_DATA_FOLDER = "data/clean_data"
DEFAULT_LOG_DIR = "logs"
DEFAULT_BACKUP_FOLDER = "data/backups"

# Elastic Search
ES_URL = "http://localhost:9200"
INDEX_NAME = "directory-index"
SHA_INDEX_NAME = "sha-check-index"