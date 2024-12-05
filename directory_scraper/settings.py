# Scrapy settings for directory_scraper project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = "directory_scraper"

SPIDER_MODULES = ["directory_scraper.spiders"]
NEWSPIDER_MODULE = "directory_scraper.spiders"


# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = "directory_scraper (+http://www.yourdomain.com)"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
#DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
#CONCURRENT_REQUESTS_PER_DOMAIN = 16
#CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "directory_scraper.middlewares.DirectoryScraperSpiderMiddleware": 543,
#}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   "directory_scraper.middlewares.DirectoryScraperDownloaderMiddleware": 543,
    'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}
# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    "directory_scraper.pipelines.DirectoryScraperPipeline": 300,
#}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
#AUTOTHROTTLE_ENABLED = True
# The initial download delay
#AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
#AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

# Set Playwright as the download handler for HTTP/HTTPS requests
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler"
}

# Optional: Playwright browser settings
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "timeout": 20 * 1000,  # 20 seconds
}
# to enable Playwright for all requests
PLAYWRIGHT_BROWSER_TYPE = "chromium"

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

LOG_LEVEL = 'ERROR'

import os
import json
from dotenv import load_dotenv
load_dotenv()

try:
    proxies_json = os.getenv("PROXIES_JSON")
    if not proxies_json:
        raise ValueError("PROXIES_JSON is not set in the environment or is empty.")
    ROTATING_PROXY_LIST = json.loads(proxies_json)
except Exception as e:
    raise RuntimeError(f"Error loading PROXIES_JSON: {e}")

ROTATING_PROXY_BAN_POLICY = 'rotating_proxies.policy.BanDetectionPolicy'

RETRY_TIMES = 5
ROTATING_PROXY_PAGE_RETRY_TIMES = 0

DOWNLOAD_TIMEOUT = 200 # seconds
CONCURRENT_REQUESTS = 16 

ROTATING_PROXY_BACKOFF_BASE = 3
ROTATING_PROXY_BACKOFF_CAP = 60

ROTATING_PROXY_REANIMATION_ENABLED = False
ROTATING_PROXY_CLOSE_ON_BAN = True
