import scrapy
from scrapy_playwright.page import PageMethod
from urllib.parse import urlparse, parse_qs, urlencode
import base64
import re

class KPKMSpider(scrapy.Spider):
    name = 'kpkm'
    allowed_domains = ['kpkm.gov.my']
    start_urls = ['https://www.kpkm.gov.my/bm/direktori-pegawai?limit=0']

    person_sort_order = 0 #init
    division_sort_order = 0 #init

    last_processed_division = None

    def __init__(self, *args, **kwargs):
        super(KPKMSpider, self).__init__(*args, **kwargs)
        self.seen_items = set()
        self.item_count = 0

    def start_requests(self):
        for url in self.start_urls:
            #print(f"Starting scrape from: {url}")
            yield scrapy.Request(
                url=url,
                callback=self.parse_page,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", 'div.person'),
                    ],
                },
            )

    async def parse_page(self, response):
        page = response.meta["playwright_page"]
        await page.wait_for_selector('div.person')

        #print(f"\nProcessing URL: {response.url}")

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

            #extract texts after <strong>, including the sibling divs
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

            #determine division and unit if both exists, and if only one exists
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

            if division != self.last_processed_division: #check if this is a new division
                self.division_sort_order += 1 
                self.last_processed_division = division

            #extract person details within this heading group.
            persons = self.get_persons_for_heading_group(group)
            for person in persons:
                person_name = person.xpath('.//div[contains(@class, "fieldname")]/span/text()').get()
                person_position = person.xpath('.//div[contains(@class, "fieldposition")]/span/text()').get()
                person_phone = person.xpath('.//div[contains(@class, "fieldtel")]//span[@class="fieldvalue"]/text()').get()

                email_element = person.xpath('.//div[contains(@class, "fieldemail")]//joomla-hidden-mail')
                person_email = self.extract_email(email_element)

                self.person_sort_order += 1

                item = {
                    'org_sort': 999,
                    'org_id': "KPKM",
                    'org_name': "KEMENTERIAN PERTANIAN DAN KETERJAMINAN MAKANAN",
                    'org_type': 'ministry',
                    'division_sort': self.division_sort_order,
                    'position_sort': self.person_sort_order,
                    'division_name': division if division else None,
                    'subdivision_name': unit if unit else None,
                    'person_name': person_name if person_name else None,
                    'position_name': person_position if person_position else None,
                    'person_phone': person_phone if person_phone else None,
                    'person_email': person_email if person_email else None,
                    'person_fax': None,
                    'parent_org_id': None, #is the parent
                    'ext_division_name': None  # Temporary field for processing

                }

                if division and " > " in division:
                    parts = division.split(" > ", 1)
                    item['division_name'] = parts[0].strip()
                    item['ext_division_name'] = parts[1].strip()
                else:                        
                    item['division_name'] = division if division else None
                    item['ext_division_name'] = None

                #if 'ext_division_name' exists, append it to 'unit_name'
                if item['ext_division_name']:
                    if item['subdivision_name']:
                        if item['subdivision_name'] != item['ext_division_name']:
                            item['subdivision_name'] = f"{item['ext_division_name']} > {item['subdivision_name']}"                            
                    else:
                        item['subdivision_name'] = item['subdivision_name']
                else:
                    item['subdivision_name'] = item['subdivision_name']

                # remove 'ext_division_name' from the item before yielding
                item.pop('ext_division_name', None)

                # Check duplicates without person_sort_order and division_sort_order
                item_tuple = (person_name, person_position, person_phone, person_email, division, unit)
                if item_tuple not in self.seen_items:
                    self.seen_items.add(item_tuple)
                    self.item_count += 1
                    #print(f"Scraped item {self.item_count}: {person_name} - {person_position} - Division: {division} - Unit: {unit}")
                    yield item

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

    def extract_email(self, email_element): #email using joomla. needs to be decode.
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

#can use last_processed_division bcs the data is already sorted row by row