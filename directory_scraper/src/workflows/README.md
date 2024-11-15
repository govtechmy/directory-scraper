# README: `main.py`

## Overview

`main.py` automates the following tasks:

1. **Running Spiders**: Executes specified web scraping spiders to collect data.  
2. **Processing Data**: Transforms the raw spider output into a clean and structured format.  
3. **Uploading Data to Elasticsearch**: Detects changes in the cleaned data and uploads it to Elasticsearch.  

```
DISCLAIMER: 
1. The uploaded data is considered the source of truth.
   - Any new data provided will completely overwrite the old data for the same `org_id`.
   - It is expected that the new data is complete and not a snippet or subset of the previous data.

2. Incomplete or partial data may result in loss of information, as the old data not present in the new upload
   will be treated as stale and deleted.

3. Ensure that the new data represents the latest information before uploading.
```

### Command

Run the script with the following command format:

```bash
python main.py <spider name | category | special keyword> [organization name] [subcategory]
```

### Examples

1. Run a Single Spider
   ```bash
   python main.py jpm
   ```

2. Run a Spider Category
   ```bash
   python main.py ministry
   ```
Refer to guide: [run_spiders.py](https://github.com/govtechmy/directory-scraper/tree/main/directory_scraper/src/data_processing) for more details.

## Workflow

The script executes the following steps in sequence:

1. **Create Required Directories**  
   The script automatically ensures that the necessary directories exist:  
   - `SPIDERS_OUTPUT_FOLDER`: Stores raw data collected by spiders.  
   - `CLEAN_DATA_FOLDER`: Stores cleaned and processed data.  
   - `BACKUP_FOLDER`: Saves backups of raw spider outputs.  

2. **Run Spiders**  
   The script executes the specified spiders based on the command-line arguments:  
   - **`<spider name | category | special keyword>`**: Specifies which spiders to run. For example:  
     - A single spider (e.g., `jpm`).  
     - A category of spiders (e.g., `ministry`).  
     - A special keyword indicating multiple spiders (e.g., `all`).  

   - **`[organization name]`** *(optional)*: Provide an organization name if the spider supports this parameter (e.g., `ministry`).  

   - **`[subcategory]`** *(optional)*: Specify subcategories, such as a product type (e.g., `jabatan`).  

   If no data is collected during this step, the script exits early.  

3. **Process Data**  
   After running the spiders, the script processes the raw JSON files into a clean and structured format:  
   - Input folder: `SPIDERS_OUTPUT_FOLDER`  
   - Output folder: `CLEAN_DATA_FOLDER`  

   The cleaned data is ready to be uploaded to ES.  

4. **Upload Data to Elasticsearch**  

Two-Level Hashing Implementation:

File-Level Hash (SHA_INDEX):
- When a file is uploaded, its entire content is hashed.
- If this hash matches the one stored in the SHA_INDEX, no further action is taken (the file hasn’t changed).
- If the hash differs, the file proceeds to Document-Level Hashing.

Document-Level Hash (sha_256_hash):
- The content of each document(row) in the file is hashed.
- The hash is compared against existing hashes in Elasticsearch.
**Actions taken:**
  - **Add**: New document (not in ES).
  - **Update**: Changed content (hash differs).
  - **Delete**: Missing document (not in the file but present in ES).
  - **No Action**: Document unchanged (hash matches).


## End Result

- New or updated data is successfully indexed in Elasticsearch. 