#script works well! but it excludes some seksyen when paginating. i will improve this by choosing "semua" instead of "10" for items shown for every page. see result48.json
import scrapy
from scrapy_playwright.page import PageMethod
from urllib.parse import urlparse, parse_qs
import base64
import re

class KPKMMainSpider(scrapy.Spider):
    name = 'kpkm'
    allowed_domains = ['kpkm.gov.my']
    start_urls = ['https://www.kpkm.gov.my/bm/direktori-pegawai']

    def __init__(self, *args, **kwargs):
        super(KPKMMainSpider, self).__init__(*args, **kwargs)
        self.seen_items = set()
        self.visited_pages = set()
        self.main_url_path = urlparse(self.start_urls[0]).path.rstrip('/')
        self.item_count = 0

    def start_requests(self):
        for url in self.start_urls:
            print(f"Starting scrape from: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_page,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", 'div.person'),
                    ],
                    "page_number": 1,
                },
            )

    async def parse_page(self, response):
        page = response.meta["playwright_page"]
        await page.wait_for_selector('div.person')

        current_page = response.meta.get("page_number", 1)
        print(f"\nProcessing page {current_page} of URL: {response.url}")

        #process each heading group (division/unit section)
        heading_groups = response.xpath('//div[@class="heading-group"]')
        for group in heading_groups:

            division = None #init
            unit = None #init

            #extract heading text
            heading_text = group.xpath('.//h4[@class="heading"]/span/text()').get()
            if heading_text:
                heading_text = heading_text.strip()
            else:
                heading_text = None

            #extract text inside <strong> tag
            strong_text = group.xpath('.//div[@class="heading-text"]//strong/text()').get()
            if strong_text:
                strong_text = strong_text.strip()
            else:
                strong_text = None

            #extract texts after <strong>, including sibling divs
            text_after_strong = []

            #get the parent element of <strong>
            strong_parent = group.xpath('.//div[@class="heading-text"]//strong/parent::*')[0]
            #get texts from the same div after <strong>
            texts_same_div = strong_parent.xpath('.//strong/following-sibling::text()').getall()
            texts_same_div = [t.strip() for t in texts_same_div if t.strip()]
            text_after_strong.extend(texts_same_div)

            #get texts from sibling divs
            following_divs = strong_parent.xpath('./following-sibling::div')
            for div in following_divs:
                texts = div.xpath('.//text()').getall()
                texts = [t.strip() for t in texts if t.strip()]
                text_after_strong.extend(texts)

            #filter out addresses and contact info
            division_candidates = []
            for text in text_after_strong:
                if not re.search(r'(Aras|Wisma|No\.|Persiaran|Presint|Putrajaya|Telefon|Faks|Malaysia|\d{5})', text, re.IGNORECASE):
                    division_candidates.append(text)

            #determine division and unit
            if strong_text:
                if division_candidates:
                    unit = strong_text
                    division = division_candidates[0]  #to use the first valid candidate
                else:
                    if heading_text and strong_text == heading_text:
                        division = strong_text
                        unit = None
                    else:
                        unit = strong_text
                        division = heading_text
            else:
                if heading_text:
                    division = heading_text
                    unit = None

            #extract person details within this heading group
            persons = self.get_persons_for_heading_group(group)
            for person in persons:
                person_name = person.xpath('.//div[contains(@class, "fieldname")]/span/text()').get()
                person_position = person.xpath('.//div[contains(@class, "fieldposition")]/span/text()').get()
                person_phone = person.xpath('.//div[contains(@class, "fieldtel")]//span[@class="fieldvalue"]/text()').get()

                email_element = person.xpath('.//div[contains(@class, "fieldemail")]//joomla-hidden-mail')
                person_email = self.extract_email(email_element)

                item = {
                    'agency': "KPKM",
                    'person_name': person_name,
                    'division': division,
                    'unit': unit,
                    'person_position': person_position,
                    'person_phone': person_phone,
                    'person_email': person_email,
                    'url': response.url,
                    'page_number': current_page
                }

                #no duplicates
                item_tuple = tuple(item.values())
                if item_tuple not in self.seen_items:
                    self.seen_items.add(item_tuple)
                    self.item_count += 1
                    print(f"Scraped item {self.item_count}: {person_name} - {person_position} - Division: {division} - Unit: {unit}")
                    yield item

        #pagination handling within the main page
        pagination_links = response.xpath('//nav[@class="pagination__wrapper"]//a[@class="page-link"]/@href').getall()

        for link in pagination_links:
            full_link = response.urljoin(link)
            parsed_url = urlparse(full_link)
            next_page_path = parsed_url.path.rstrip('/')

            #check if the path is exactly the same as the main URL's path
            if next_page_path == self.main_url_path:
                if full_link not in self.visited_pages:
                    self.visited_pages.add(full_link)
                    query_params = parse_qs(parsed_url.query)
                    start_param = query_params.get('start', [0])[0]
                    next_page_number = int(int(start_param) / 10) + 1  #assuming 10 items per page
                    print(f"Next page URL: {full_link}")
                    yield scrapy.Request(
                        url=full_link,
                        callback=self.parse_page,
                        meta={
                            "playwright": True,
                            "playwright_include_page": True,
                            "playwright_page_methods": [
                                PageMethod("wait_for_selector", 'div.person'),
                            ],
                            "page_number": next_page_number,
                        },
                        dont_filter=True
                    )
            else:
                print(f"Skipping link outside main URL: {full_link}")

        await page.close()

    def get_persons_for_heading_group(self, group):
        persons = []
        siblings = group.xpath('following-sibling::*')
        for sibling in siblings:
            if sibling.xpath('self::div[@class="heading-group"]'):
                break
            person_elements = sibling.xpath('.//div[contains(@class, "person")]')
            if person_elements:
                persons.extend(person_elements)
        return persons

    def extract_email(self, email_element):
        if not email_element:
            return None

        first = email_element.xpath('@first').get()
        last = email_element.xpath('@last').get()
        text = email_element.xpath('@text').get()

        if text:
            return self.b64_decode_unicode(text)
        elif first and last:
            return f"{self.b64_decode_unicode(first)}@{self.b64_decode_unicode(last)}"
        else:
            return None

    @staticmethod
    def b64_decode_unicode(encoded_str):
        try:
            decoded = base64.b64decode(encoded_str).decode('utf-8')
            return decoded
        except Exception as e:
            print(f"Error decoding: {e}")
            return None
