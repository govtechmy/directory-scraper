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

def backup_spider_outputs(output_folder, spider_names, backup_folder, max_backups=3):
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

def run_spiders(spider_list, output_folder, backup_folder):
    global success_count, fail_count, success_spiders, fail_spiders
    success_count, fail_count = 0, 0
    success_spiders, fail_spiders = [], []

    spider_loader = SpiderLoader.from_settings(get_project_settings())
    all_spiders = spider_loader.list()

    backup_spider_outputs(output_folder=output_folder, spider_names=spider_list, backup_folder=backup_folder)
    setup_output_folder(folder_path=output_folder, spider_names=spider_list)

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

#========= SPIDER TREE FUNCTIONS based on spiders/ folder hierarchy ===============

def validate_path(spider_tree, *path_parts):
    """
    Validates that each path part exists in the spider tree at each level.
    Returns a tuple (is_valid, level) where `is_valid` is True if the full path is valid,
    and `level` is the spider tree level at the specified path.
    """
    current_level = spider_tree
    for part in path_parts:
        if isinstance(current_level, dict) and part in current_level:
            current_level = current_level[part]
        else:
            return False, None  # Path is invalid
    return True, current_level  # Path is valid

def build_spider_tree(base_dir=None):
    """
    Builds the spider tree (dictionary) based on the folder structure under `spiders/`.
    """
    settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(settings)
    all_spiders = spider_loader.list()

    spider_modules = settings.get('SPIDER_MODULES', [])
    spider_tree = {}

    if spider_modules:
        module_path = spider_modules[0].replace(".", os.sep)
        base_dir = os.path.abspath(os.path.join(os.getcwd(), module_path))
    else:
        print("SPIDER_MODULES is not configured correctly.")
        return spider_tree

    for spider_name in all_spiders:
        spider_cls = spider_loader.load(spider_name)
        spider_module = spider_cls.__module__
        spider_file_path = os.path.abspath(spider_module.replace(".", os.sep) + ".py")
        relative_spider_file_path = os.path.relpath(spider_file_path, base_dir)

        path_parts = relative_spider_file_path.split(os.sep)
        current_level = spider_tree
        for part in path_parts[:-1]:  # Ignore the file itself
            current_level = current_level.setdefault(part, {})
        current_level.setdefault("spiders", []).append(spider_name)

    return spider_tree

def extract_spiders_from_path(spider_tree, *path_parts):
    """
    Navigates the spider tree to extract all spiders at the specified path level,
    collecting spiders from any nested subdirectories.
    """
    _, level = validate_path(spider_tree, *path_parts)
    if not level:
        return []

    spiders = []
    def collect_spiders(level):
        if isinstance(level, list):  # If level is a list, directly add its items
            spiders.extend(level)
        elif isinstance(level, dict):  # If level is a dictionary, continue traversing
            if 'spiders' in level:
                spiders.extend(level['spiders'])
            for sub_level in level.values():
                collect_spiders(sub_level)

    collect_spiders(level)
    return spiders

def get_all_spiders():
    settings = get_project_settings()
    spider_loader = SpiderLoader.from_settings(settings)
    return spider_loader.list()

#================ END OF SPIDER TREE FUNCTION ==============

#================ ARG VALIDATION FUNCTION ==================

def validate_arg_name(name, all_spiders, spider_tree):
    """Validate 'name' argument."""
    if name not in all_spiders and name not in spider_tree and name not in ["all", "list"]:
        print(f"Error: '{name}' is not a valid spider, category, or special keyword.")
        print("Please provide a valid spider name, category (e.g., 'ministry', 'ministry_orgs'), or special keyword ('all' or 'list').")
        return False
    return True

def validate_arg_org_name(name, org_name, spider_tree):
    """Validate 'org_name' argument if 'name' is a valid category."""
    if name in spider_tree and org_name:
        if org_name not in spider_tree[name]:
            print(f"Error: '{org_name}' is not a valid organization under the '{name}' category.")
            print(f"Available organizations in '{name}': {', '.join(spider_tree[name].keys())}")
            return False
    return True

def validate_arg_subcategory(name, org_name, subcategory, spider_tree):
    """Validate 'subcategory' argument if 'name' and 'org_name' are valid."""
    if name in spider_tree and org_name and subcategory:
        if subcategory not in spider_tree[name][org_name]:
            print(f"Error: '{subcategory}' is not a valid subcategory under the organization '{org_name}' in '{name}'.")
            print(f"Available subcategories in '{org_name}': {', '.join(spider_tree[name][org_name].keys())}")
            return False
    return True

def validate_extra_args(name, org_name, subcategory, all_spiders):
    """
    Ensure no extra arguments are passed for single spiders, 'all', or 'list' when user input the arg name.
    """
    if name in all_spiders or name in ["all", "list"]:
        if org_name or subcategory:
            print(f"Error: '{name}' is not a category. Extra arguments are only applicable for categories.")
            print("Refer to `python run_spiders.py --help` for usage guidelines.")
            return False
    return True

#========== END OF ARG VALIDATION FUNCTION =============

def main():
    parser = argparse.ArgumentParser(
        description=""" 
    Run Scrapy spiders based on a structured hierarchy.

    Usage:
    ------
    python run_spiders.py <spider name | category | special keyword> [organization name] [subcategory]
    
    *Note*: No extra arguments (`organization name` or `subcategory`) can be used with a <spider name> and <special keywords>.

    Arguments:
    ----------
    1. <spider name | category | special keyword> (REQUIRED):
    - Spider Name: Specify a specific spider by its name (e.g., 'jpm', 'mof', 'mohr').
    - Category:
        • 'ministry'      : Runs all spiders under the 'ministry' directory.
        • 'ministry_orgs' : Runs all spiders under the 'ministry_orgs' directory.
        • 'non_ministry'  : Runs all spiders under the 'non_ministry' directory.
        • 'bahagian_unit' : Runs all spiders under the 'bahagian_unit' directory.
    - Special Keywords: 
        • 'all'           : Runs every available spider.
        • 'list'          : Runs a predefined list of spiders in the code. 

    2. [organization name] (OPTIONAL): Specify an organization within the category if applicable (e.g., 'jpm', 'mohr').

    3. [subcategory] (OPTIONAL): Provide a specific subcategory under the organization (e.g., 'jabatan', 'agensi').

    *Note*: OPTIONAL arguments only applies for <Category>
    
    Examples:
    ---------
    To run a specific spider by its spider name:
    python run_spiders.py jpm

    To run all `ministry` category spiders:
    python run_spiders.py ministry
    
    To run all spiders under the `ministry_orgs` category for the `jpm` organization:
    python run_spiders.py ministry_orgs jpm

    To run only the spiders under the `ministry_orgs` category for the `jpm` organization within the `jabatan` subcategory:
    python run_spiders.py ministry_orgs jpm jabatan

    To run a predefined list of spiders:
    python run_spiders.py list
    """,
        formatter_class=argparse.RawTextHelpFormatter 

    )
    parser.add_argument("name", help="Specify the spider name, category, or use 'all' for all spiders. See above for options.")
    parser.add_argument("org_name", nargs="?", default=None, help="(Optional) Specify the organisation name if applicable. (e.g. 'jpm', or 'mohr')")
    parser.add_argument("subcategory", nargs="?", default=None, help="(Optional) Specify the subcategory if applicable. (e.g 'jabatan', or 'agensi')")

    args = parser.parse_args()
    spider_tree = build_spider_tree()
    all_spiders = get_all_spiders()
    logger.debug(f"Spider tree: {spider_tree}")

    LIST_OF_SPIDERS_TO_RUN = ["jpm", "mof", "nadma", "felda", "perkeso", "niosh", "banknegara", "petronas"]

    #================== ARGS VALIDATION =====================
    if not validate_arg_name(args.name, all_spiders, spider_tree):
        return
    if not validate_extra_args(args.name, args.org_name, args.subcategory, all_spiders):
        return
    if not validate_arg_org_name(args.name, args.org_name, spider_tree):
        return
    if not validate_arg_subcategory(args.name, args.org_name, args.subcategory, spider_tree):
        return
    #=========================================================

    # Determine the list of spiders to run
    if args.name in all_spiders:
        spider_list = [args.name]
    elif args.name == "all":
        spider_list = all_spiders
    elif args.name == "list":
        spider_list = [spider for spider in LIST_OF_SPIDERS_TO_RUN if spider in all_spiders]
    else:
        # Validate path and extract spiders from tree
        path_parts = [part for part in [args.name, args.org_name, args.subcategory] if part]
        is_valid, _ = validate_path(spider_tree, *path_parts)
        if not is_valid:
            print(f"Invalid path: {'/'.join(path_parts)}. Please check available categories.")
            return

        spider_list = extract_spiders_from_path(spider_tree, *path_parts)

    if not spider_list:
        print(f"No spiders found for specified path: {'/'.join(path_parts)}")
        return

    print(f"Running spiders: {spider_list}")

    OUTPUT_FOLDER = "./data/spiders_output"
    BACKUP_FOLDER = "./backups"

    run_spiders(spider_list, output_folder=OUTPUT_FOLDER, backup_folder=BACKUP_FOLDER)

if __name__ == "__main__":
    main()