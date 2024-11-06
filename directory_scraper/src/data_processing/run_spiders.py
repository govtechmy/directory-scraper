import json
import os
import shutil
import logging
import re
from datetime import datetime
import argparse
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.spiderloader import SpiderLoader
import inspect


#==========================Logging setup=======================================
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LOG_DIR = os.path.join(os.path.dirname(__file__), '../..', 'logs')
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)
LOG_FILE_NAME = 'run_spiders.log'
LOG_FILE_PATH = os.path.join(LOG_DIR, LOG_FILE_NAME)

file_handler = logging.FileHandler(LOG_FILE_PATH, mode='w')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.propagate = True

#==============================================================================

# Global success and fail counters
success_count = 0
fail_count = 0
success_spiders = []
fail_spiders = []

def filter_custom_logs(LOG_FILE_PATH=LOG_FILE_PATH):
    """
    This function filters out custom logs from the main log file (LOG_FILE_PATH),
    and writes only the entries generated by the custom logger (identified using the __name__ attribute) into a new log file, 
    excluding Scrapy's internal logs (identified using regex pattern) (e.g., [scrapy.core.engine], [scrapy-playwright], etc)

    Reason: Scrapy manages the entire logging system for spiders, making it hard to isolate custom logs from Scrapy's logs during runtime. 
    This function filters the logs post-run, allowing us to separate and extract only the custom logs based on the module's name (__name__).

    Arguments:
    LOG_FILE_PATH : str (optional)
    The path to the main log file to filter (defaults to LOG_FILE_PATH).
    """
    input_log = LOG_FILE_PATH
    output_log = LOG_FILE_PATH.replace('.log', '_custom.log')
    custom_logger_name = f"[{__name__}]"
    scrapy_pattern = re.compile(r"\[scrapy(?:\..*?)?\]")
    non_scrapy_pattern = re.compile(r", (DEBUG|INFO|WARNING|ERROR|CRITICAL) - .* - ")

    try:
        with open(input_log, "r") as infile, open(output_log, "w") as outfile:
            for line in infile:
                if scrapy_pattern.search(line):
                    continue
                elif custom_logger_name in line:
                    outfile.write(line)
                elif non_scrapy_pattern.search(line) and "scrapy" not in line:
                    outfile.write(line)
                elif not scrapy_pattern.search(line) and "scrapy" not in line:
                    outfile.write(line)
        logger.info(f"Filtered custom logs have been written to {output_log}")
    except FileNotFoundError:
        logger.error(f"The file {input_log} does not exist.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

def setup_output_folder(folder_path, spider_names):
    """
    - Prepares the output folder for storing spider results.
    - Creates the output folder if it doesn't exist.
    - Deletes any files or directories in the folder that match the spider names.

    Args:
    folder_path (str): The path to the folder where spider output will be stored.
    spider_names (list): List of spider names whose related files/folders need to be removed.
    """
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    else:
        for f in os.listdir(folder_path):
            file_path = os.path.join(folder_path, f)
            file_name, file_ext = os.path.splitext(f)
            try:
                if file_name in spider_names and os.path.isfile(file_path):
                    os.remove(file_path)
                    logger.info(f"Deleted file: {file_path}")
                elif file_name in spider_names and os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    logger.info(f"Deleted directory: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to delete {file_path}. Reason: {e}")
        logger.info(f"Done deleting relevant files in {folder_path}")

def backup_spider_outputs(output_folder, spider_names, backup_folder, max_backups=5):
    """
    Backs up existing spider output files to a backup folder.
    Creates timestamped backups and removes older backups if exceeding max_backups.

    Args:
    output_folder (str): Path to the folder containing the spider output files.
    spider_names (list): List of spider names whose output should be backed up.
    backup_folder (str): Path to the folder where backups will be stored.
    max_backups (int): Maximum number of backups to retain for each spider outputs. 
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)

    for spider_name in spider_names:
        spider_output_file = os.path.join(output_folder, f"{spider_name}.json")
        if os.path.exists(spider_output_file):
            spider_backup_folder = os.path.join(backup_folder, spider_name)
            if not os.path.exists(spider_backup_folder):
                os.makedirs(spider_backup_folder)

            backup_file_path = os.path.join(spider_backup_folder, f"{spider_name}_{timestamp}.json")
            try:
                shutil.copy(spider_output_file, backup_file_path)
                logger.info(f"Backed up {spider_output_file} to {backup_file_path}")
            except Exception as e:
                logger.error(f"Error backing up {spider_output_file}: {e}")

            backups = sorted(os.listdir(spider_backup_folder), reverse=True)
            if len(backups) > max_backups:
                backups_to_remove = backups[max_backups:]
                for backup in backups_to_remove:
                    os.remove(os.path.join(spider_backup_folder, backup))
                    logger.info(f"Removed old backup: {backup}")

def setup_crawler(spiders):
    """
    Sets up and configures the Scrapy crawler with custom settings.
    Configures retry attempts, timeouts, logging, and integrates custom pipelines.

    Args:
    spiders (list): List of spiders for crawling.

    Returns:
    CrawlerProcess: A Scrapy CrawlerProcess instance with configured settings.
    """
    settings = get_project_settings()
    settings.set('RETRY_ENABLED', True)
    settings.set('RETRY_TIMES', 3)
    settings.set('DOWNLOAD_TIMEOUT', 60)
    settings.set('DOWNLOAD_DELAY', 1)
    settings.set('LOG_FILE', LOG_FILE_PATH)
    settings.set('LOG_LEVEL', 'INFO')

    set_playwright_settings(settings, spiders)
    process = CrawlerProcess(settings)

    return process

def set_playwright_settings(settings, spiders):
    """
    If any spider has Playwright enabled, the function sets the Playwright 
    to run in headless mode (True).

    Args:
    settings (Settings): Scrapy settings object to configure.
    spiders (list): List of spider names (to check for Playwright support)
    """
    spider_loader = SpiderLoader.from_settings(settings)
    for spider_name in spiders:
        spider_cls = spider_loader.load(spider_name)
        if hasattr(spider_cls, 'playwright_enabled') and spider_cls.playwright_enabled:
            settings.set('PLAYWRIGHT_LAUNCH_OPTIONS', {"headless": True})
            logger.info(f"Playwright detected for spider '{spider_name}', forcing headless mode.")
            break

class RunSpiderPipeline:
    """
    Pipeline to collect and store the results when a spider crawls website.

    Attributes:
        results (dict): Stores scraped items for each spider.

    Methods:
        open_spider(spider): Initializes results for the spider and logs its start.
        process_item(item, spider): Appends scraped items to the spider's results.
        close_spider(spider): Writes results to a JSON file if data was collected, or logs a failure.
    """
    def __init__(self):
        self.results = {}

    def open_spider(self, spider):
        self.start_time = datetime.now()
        global success_count, fail_count, success_spiders, fail_spiders
        self.results[spider.name] = []
        logger.info(f"Running spider '{spider.name}' ...")

    def process_item(self, item, spider):
        self.results[spider.name].append(item)
        return item

    def close_spider(self, spider):
        end_time = datetime.now()
        duration = end_time - self.start_time
        logger.info(f"Finished spider '{spider.name}'. Duration: {duration}")

        if self.results[spider.name]:
            output_file = os.path.join("./data/spiders_output", f"{spider.name}.json")
            with open(output_file, 'w') as f:
                f.write("[\n")
                for idx, result in enumerate(self.results[spider.name]):
                    json.dump(result, f)
                    if idx < len(self.results[spider.name]) - 1:
                        f.write(",\n")
                    else:
                        f.write("\n")
                f.write("]\n")
            global success_count, success_spiders
            success_count += 1
            success_spiders.append(spider.name)
            logger.info(f"Spider '{spider.name}' finished successfully.")
        else:
            global fail_count, fail_spiders
            fail_count += 1
            fail_spiders.append(spider.name)
            logger.warning(f"Spider '{spider.name}' finished and failed to collect any data.")

def run_spiders(spider_list):
    global success_count, fail_count, success_spiders, fail_spiders
    success_count, fail_count = 0, 0
    success_spiders, fail_spiders = [], []

    spider_loader = SpiderLoader.from_settings(get_project_settings())
    all_spiders = spider_loader.list()

    backup_spider_outputs(output_folder="./data/spiders_output", spider_names=spider_list, backup_folder="./backups")
    setup_output_folder(folder_path="./data/spiders_output", spider_names=spider_list)

    process = setup_crawler(spider_list)
    process.settings.set('ITEM_PIPELINES', {'__main__.RunSpiderPipeline': 1})

    for spider_name in spider_list:
        if spider_name in all_spiders:
            spider_cls = spider_loader.load(spider_name)
            process.crawl(spider_cls)
        else:
            logger.warning(f"Spider '{spider_name}' not found. Skipping...")

    process.start()
    logger.info(f"SUCCESSFUL: {success_count} spiders. Spiders: {success_spiders}")
    logger.info(f"FAILED: {fail_count} spiders. Spiders: {fail_spiders}")

def get_spiders_by_folder():
    """
    Function to categorize the spider scripts according to the folders in spiders/.
    Modify this function if new folder(category) is added!
    Current category:
    1. ministry
    2. ministry_orgs
    3. non_ministry
    """
    settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(settings)
    all_spiders = spider_loader.list()

    spiders_by_category = {
        "ministry": [],
        "ministry_orgs": [],
        "non_ministry": []
    }

    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../spiders"))
    logger.debug(f"Base directory: {base_dir}")

    for spider_name in all_spiders:
        spider_cls = spider_loader.load(spider_name)
        
        # Use inspect to get the actual file path of the spider class
        spider_file_path = os.path.abspath(inspect.getfile(spider_cls))
        
        # Ensure the file path is relative to the base_dir
        if os.path.exists(spider_file_path):
            relative_spider_file_path = os.path.relpath(spider_file_path, base_dir)
            logger.debug(f"Spider name: {spider_name}, Relative Path: {relative_spider_file_path}")

            # Check which category the spider belongs to
            if relative_spider_file_path.startswith("ministry" + os.sep):
                spiders_by_category["ministry"].append(spider_name)
            elif relative_spider_file_path.startswith("ministry_orgs" + os.sep):
                spiders_by_category["ministry_orgs"].append(spider_name)
            elif relative_spider_file_path.startswith("non_ministry" + os.sep):
                spiders_by_category["non_ministry"].append(spider_name)

    return spiders_by_category

def get_all_spiders():
    settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(settings)
    return spider_loader.list()

def main():
    parser = argparse.ArgumentParser(
        description="Run Scrapy spiders for ministries, ministry organizations, and non-ministry entities.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("name", help="Specify the spider name, category, or 'all'. Options include:\n"
                                     "Spider name:\n"
                                     "- A specific spider name (e.g., 'jpm', 'mof', 'mohr', etc.).\n"
                                     "Category:\n"
                                     "- 'ministry': Runs all spiders in the 'ministry' folder.\n"
                                     "- 'ministry_orgs': Runs all spiders in the 'ministry_orgs' folder.\n"
                                     "- 'non_ministry': Runs all spiders in the 'non_ministry' folder.\n"
                                     "All:\n"
                                     "- 'all': Runs all available spiders.\n"
                                     )
    parser.add_argument("org_name", nargs="?", default=None, help="Specify the organisation name under the main category if applicable (e.g., 'jpm', 'mohr')")
    parser.add_argument("subcategory", nargs="?", default=None, help="Specify the subcategory if applicable (e.g., 'jabatan', 'agensi').")

    args = parser.parse_args()
    spiders = get_spiders_by_folder()

    all_spiders = get_all_spiders()

    LIST_OF_SPIDERS_TO_RUN = ["jpm", "mof", "nadma", "felda", "perkeso", "niosh", "banknegara", "petronas"]

    if args.name in all_spiders:
        spider_list = [args.name]
    elif args.name in spiders["ministry"]:
        spider_list = [args.name]
    elif args.name == "ministry":
        spider_list = spiders["ministry"]
    elif args.name == "ministry_orgs":
        spider_list = spiders["ministry_orgs"]
    elif args.name == "non_ministry":
        spider_list = spiders["non_ministry"]
    elif args.name == "all":
        spider_list = all_spiders #spiders["ministry"] + spiders["ministry_orgs"] + spiders["non_ministry"]
    elif args.name == "list":
        spider_list = [spider for spider in LIST_OF_SPIDERS_TO_RUN if spider in all_spiders]
    else:
        logger.error(f"Invalid spider name or category specified: {args.name}")
        print(f"Invalid spider name or category specified: {args.name}")
        return

    print(f"Running spider: {spider_list}")
    run_spiders(spider_list)

if __name__ == "__main__":
    main()