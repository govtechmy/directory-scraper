import scrapy
from scrapy_playwright.page import PageMethod
import asyncio
from scrapy.selector import Selector

class KuskopcsrfSpider(scrapy.Spider):
    name = 'kuskop'
    allowed_domains = ['kuskop.gov.my']
    start_urls = ['https://www.kuskop.gov.my/index.php?id=11&page_id=27']

    person_sort_order = 1

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

        self.logger.info("Extracting all 'bahagian' options dynamically.")
        content = await page.content()
        selector = Selector(text=content)

        options = selector.xpath('//select[@id="pilihbahagian"]/option[@value != ""]/attribute::value').getall()
        self.logger.info(f"Found {len(options)} options for 'bahagian'.")

        for option_value in options:
            self.logger.info(f"Selecting option '{option_value}' from '#pilihbahagian'.")
            await page.select_option('#pilihbahagian', option_value)

            self.logger.info("Waiting for network idle state after selecting option.")
            await page.wait_for_load_state('networkidle')
            #await asyncio.sleep(2)

            self.logger.info("Clicking the search button.")
            await page.click('button.btn-default[type="submit"]:has-text("CARI")')

            self.logger.info("Waiting for network idle state after clicking the search button.")
            await page.wait_for_load_state('networkidle')
            #await asyncio.sleep(2)

            self.logger.info(f"Taking a snapshot of the page content for option '{option_value}'.")
            content = await page.content()

            # # Save the content for debugging (optional)
            # with open(f'response_content_option_{option_value}.html', 'w', encoding='utf-8') as file:
            #     file.write(content)

            self.logger.info(f"Parsing content for option '{option_value}'.")
            selector = Selector(text=content)

            #use async for to iterate over the yielded items from parse_results
            async for item in self.parse_results(selector, option_value):
                yield item

        await page.close()

    async def parse_results(self, selector, option_value):
        self.logger.info("Starting to parse directory entries.")
        
        panels = selector.xpath('//div[@class="col-sm-6 col-md-8"]')
        panel_count = len(panels)
        self.logger.info(f"Found {panel_count} panels matching the structure for option '{option_value}'.")

        if panel_count == 0:
            self.logger.warning(f"No panels found for option '{option_value}'. Please check the HTML structure or XPath.")
            return

        for index, panel in enumerate(panels, start=1):
            name = panel.xpath('.//h5/text()').get()
            position = panel.xpath('.//small[1]/text()').get()
            division_unit = panel.xpath('.//span/small/text()').getall()
            phone = panel.xpath('.//i[contains(@class, "fa-phone")]/following-sibling::text()[1]').get()
            fax = panel.xpath('.//i[contains(@class, "fa-fax")]/following-sibling::text()[1]').get()
            email = panel.xpath('.//i[contains(@class, "fa-envelope")]/following-sibling::text()[1]').get()

            division, unit = (division_unit[0], division_unit[1]) if len(division_unit) > 1 else (division_unit[0], None)

            item = {
                'org_sort': 999,
                'org_id': "KUSKOP",
                'org_name': "KEMENTERIAN PEMBANGUNAN USAHAWAN DAN KOPERASI",
                'org_type': 'ministry',
                'division_sort': int(option_value),
                'position_sort': self.person_sort_order,
                'division_name': division.strip() if division else None,
                'subdivision_name': unit.strip() if unit else None,
                'person_name': name.strip() if name else None,
                'position_name': position.strip() if position else None,
                'person_phone': phone.strip() if phone else None,
                'person_email': (email.strip() + '@kuskop.gov.my') if email else None,
                'person_fax': fax.strip() if fax else None,
                'parent_org_id': None, #is ministry
            }

            self.person_sort_order += 1

            #self.logger.info(f"Parsed panel {index}/{panel_count} for option '{option_value}': {item['person_name']} - {item['position_name']}")

            yield item
        
        self.logger.info(f"Finished parsing all panels for option '{option_value}'.")

    def handle_error(self, failure):
        response = failure.value.response
        if response:
            self.logger.error(f"Request failed with status {response.status}")
            self.logger.error(f"Response body: {response.text}")
