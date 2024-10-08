import scrapy
from scrapy_playwright.page import PageMethod
from scrapy import FormRequest
import asyncio

class KuskopcsrfSpider(scrapy.Spider):
    name = 'kuskop'
    allowed_domains = ['kuskop.gov.my']
    start_urls = ['https://www.kuskop.gov.my/index.php?id=11&page_id=27']

    custom_settings = {
        'PLAYWRIGHT_BROWSER_TYPE': 'chromium',
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': False},
        
    }

    def parse(self, response):
        yield scrapy.Request(
            response.url,
            meta={
                'playwright': True,
                'playwright_include_page': True,
                'playwright_page_methods': [
                    PageMethod('wait_for_selector', '#pilihbahagian')
                ]
            },
            callback=self.interact_with_page
        )

    async def interact_with_page(self, response):
        page = response.meta['playwright_page']

        # Select an option from the "bahagian" dropdown
        await page.select_option('#pilihbahagian', '1')
        self.logger.info("Bahagian option selected: '1'")

        await page.wait_for_load_state('networkidle')

        await asyncio.sleep(2)

        # Wait for the "seksyen" dropdown to appear and select an option
        await page.wait_for_selector('#pilihseksyen')

        await page.select_option('#pilihseksyen', '')
        self.logger.info("Seksyen option selected: ''")

        await page.wait_for_load_state('networkidle')

        await asyncio.sleep(5)

        # Click the search button after selecting the dropdown option
        await page.click('button.btn-default[type="submit"]:has-text("CARI")')
        self.logger.info("Search button clicked")

        await asyncio.sleep(5)

        # Wait for the network requests to complete after clicking
        await page.wait_for_load_state('networkidle')
        
        await asyncio.sleep(5)

        # Extract cookies after the button click
        cookies = await page.context.cookies()
        csrf_token = None
        for cookie in cookies:
            if cookie['name'] == '_csrf':
                csrf_token = cookie['value']
                break

        if not csrf_token:
            self.logger.error("CSRF token not found in cookies after button click. Aborting.")
            await page.close()
            return

        self.logger.info(f"CSRF token found: {csrf_token}")

        # Take a snapshot of the page after clicking the button
        content = await page.content()

        # Save the content to a file for debugging
        with open('response_content.html', 'w', encoding='utf-8') as file:
            file.write(content)

        # Replace the response body with the updated content
        self.parse_results(response.replace(body=content))

        # Close the page to release resources
        await page.close()

    def parse_results(self, response):
        for row in response.xpath('//div[contains(@class, "row record")]'):
            item = {
                'name': row.xpath('.//div[contains(@class, "col-md-4")]/p/strong/text()').get(),
                'position': row.xpath('.//div[contains(@class, "col-md-2")][1]/p/text()').get(),
                'office': row.xpath('.//div[contains(@class, "col-md-2")][2]/p/italic/text()').get(),
                'phone': row.xpath('.//div[contains(@class, "col-md-2")][3]/p//text()').getall(),
                'email': row.xpath('.//div[contains(@class, "col-md-2")][4]/p/text()').get(),
            }
            self.logger.info(f"Extracted item: {item}")
            yield item

    def handle_error(self, failure):
        response = failure.value.response
        if response:
            self.logger.error(f"Request failed with status {response.status}")
            self.logger.error(f"Response body: {response.text}")
