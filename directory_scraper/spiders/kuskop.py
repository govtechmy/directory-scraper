import scrapy
from scrapy_playwright.page import PageMethod
import asyncio
from scrapy.selector import Selector

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
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True},
    }

    def parse(self, response):
        self.logger.info("Starting the parsing process with the initial request.")
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
        self.logger.info("Routing requests to block Google Analytics.")
        await page.route("**/*", lambda route, request: route.abort() if "google-analytics.com" in request.url else route.continue_())

        self.logger.info("Selecting option '19' from '#pilihbahagian'.")
        await page.select_option('#pilihbahagian', '19')

        self.logger.info("Waiting for network idle state after selecting option.")
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(2)

        self.logger.info("Clicking the search button.")
        await page.click('button.btn-default[type="submit"]:has-text("CARI")')

        self.logger.info("Waiting for network idle state after clicking the search button.")
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(2)

        self.logger.info("Taking a snapshot of the page content.")
        content = await page.content()

        #save as a file for debugging
        with open('response_content.html', 'w', encoding='utf-8') as file:
            file.write(content)

        #create a Selector object directly from the page content
        self.logger.info("Parsing the content directly.")
        selector = Selector(text=content)

        # Locate all panels containing staff information
        panels = selector.xpath('//div[@class="col-sm-6 col-md-8"]')
        panel_count = len(panels)
        self.logger.info(f"Found {panel_count} panels matching the structure.")

        if panel_count == 0:
            self.logger.warning("No panels found. Please check the HTML structure or XPath.")
            return

        for index, panel in enumerate(panels, start=1):
            # Extract fields using XPath and convert them to strings
            name = panel.xpath('.//h5/text()').get()
            position = panel.xpath('.//small[1]/text()').get()
            division_unit = panel.xpath('.//span/small/text()').getall()
            phone = panel.xpath('.//i[contains(@class, "fa-phone")]/following-sibling::text()[1]').get()
            fax = panel.xpath('.//i[contains(@class, "fa-fax")]/following-sibling::text()[1]').get()
            email = panel.xpath('.//i[contains(@class, "fa-envelope")]/following-sibling::text()[1]').get()

            #separate division and unit if both are present
            division, unit = (division_unit[0], division_unit[1]) if len(division_unit) > 1 else (division_unit[0], None)

            item = {
                'person_name': name.strip() if name else None,
                'person_position': position.strip() if position else None,
                'division_name': division.strip() if division else None,
                'unit_name': unit.strip() if unit else None,
                'person_phone': phone.strip() if phone else None,
                'person_fax': fax.strip() if fax else None,
                'person_email': (email.strip() + '@kuskop.gov.my') if email else None
            }

            self.logger.info(f"Parsed panel {index}/{panel_count}: {item['name']} - {item['position']}")

            yield item
        
        self.logger.info("Finished parsing all panels.")
        await page.close()

    def handle_error(self, failure):
        response = failure.value.response
        if response:
            self.logger.error(f"Request failed with status {response.status}")
            self.logger.error(f"Response body: {response.text}")
