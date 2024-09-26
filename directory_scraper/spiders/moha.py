import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import re
from urllib.parse import urlparse, parse_qs

class MohaSpider(CrawlSpider):
    name = 'moha_spider'
    allowed_domains = ['www.moha.gov.my']
    start_urls = ['https://www.moha.gov.my/index.php/ms/kdn1/dir-kdn']

    rules = (
        Rule(LinkExtractor(allow=r'/index\.php/ms/kdn1/dir-kdn/\d+-'), callback='parse_item', follow=True),
        Rule(LinkExtractor(allow=r'/index\.php/ms/kdn1/dir-kdn\?start=\d+'), follow=True),
    )

    def extract_page(self, url):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        if 'start' in query_params:
            start_value = int(query_params.get('start')[0])
            page_number = (start_value // 10) + 1
            return page_number
        return 1

    def extract_name(self, name_text):
        if name_text:
            return name_text.split('|')[0].strip()
        return ''

    def extract_phone(self, phone_text):
        if phone_text:
            match = re.search(r'Telefon:\s*(.+)', phone_text)
            if match:
                return match.group(1).strip()
        return ''

    def parse_item(self, response):
        page_number = self.extract_page(response.url)
        division = response.css('h2::text').get()
        if division:
            division = division.strip()

        for item in response.css('ul.category li'):
            person_name = self.extract_name(item.css('div.list-title a::text').get())
            person_position_list = item.css('div.list-title::text').getall()
            person_position = ' '.join([text.strip() for text in person_position_list if text.strip()])
            person_position = re.sub(r'\s+', ' ', person_position).strip()
            person_phone_text = item.css('div.span3::text').get()
            person_phone = self.extract_phone(person_phone_text)

            yield {
                'agency_id': 'MOHA',
                'agency': 'KEMENTERIAN DALAM NEGERI',
                'person_name': person_name,
                'division': division,
                'unit': '',
                'person_position': person_position,
                'person_phone': person_phone,
                #'url': response.url,
                #'page_number': page_number,
                #'person_email': person_email #to fix
            }

