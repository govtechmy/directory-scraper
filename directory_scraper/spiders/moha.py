import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import re
from urllib.parse import urlparse, parse_qs
import time
import random
from html import unescape  # Import to decode HTML entities

class MohaSpider(CrawlSpider):
    name = 'moha'
    allowed_domains = ['www.moha.gov.my']
    start_urls = ['https://www.moha.gov.my/index.php/ms/kdn1/dir-kdn']

    # custom settings for this spider (moha website will close connection if no delay when scraping)
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408],
        'RETRY_WAIT_TIME': 10,
        'DOWNLOAD_TIMEOUT': 30,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'LOG_LEVEL': 'ERROR',  # Use 'DEBUG' for more detailed logs
    }

    division_sort_map = {
        "Pejabat Menteri": 1,
        "Pejabat Timbalan Menteri": 2,
        "Pejabat Ketua Setiausaha": 3,
        "Pejabat Timbalan Ketua Setiausaha (Keselamatan)": 4,
        "Pejabat Timbalan Ketua Setiausaha (Pengurusan) - Ketua Pegawai Maklumat (CIO)": 5,
        "Pejabat Timbalan Ketua Setiausaha (Dasar dan Kawalan)": 6,
        "Bahagian Keselamatan dan Ketenteraman Awam": 7,
        "Bahagian Penguatkuasaan dan Kawalan": 8,
        "Bahagian Pengurusan Teknologi Maklumat": 9,
        "Bahagian Pengurusan Sumber Manusia": 10,
        "Bahagian Antarabangsa": 11,
        "Bahagian Kepenjaraan, Antidadah dan RELA": 12,
        "Pejabat Penasihat Undang-Undang": 13,
        "Bahagian Audit Dalam": 14,
        "Bahagian Khidmat Pengurusan": 15,
        "Bahagian Hal Ehwal Imigresen": 16,
        "Pejabat Strategik Nasional (NSO) MAPO": 17,
        "Unit Komunikasi Korporat": 18,
        "Bahagian Perancangan Strategik": 19,
        "Bahagian Pembangunan": 20,
        "Bahagian Kewangan": 21,
        "Pejabat Penapisan Filem": 22,
        "Bahagian Akaun": 23,
        "Bahagian Perolehan": 24,
        "Bahagian Pendaftaran Negara dan Pertubuhan": 25,
        "Institut Keselamatan Awam Malaysia": 26,
        "Suruhanjaya Pasukan Polis": 27,
        "Unit Integriti": 28,
        "Bahagian Urus Setia Lembaga Parol": 29,
        "Bahagian Pengurusan Kawal Selia Pencegahan Jenayah dan Keganasan": 30,
        "Pejabat Pasukan Petugas Khas NIISe": 31,
        "Agensi Kawalan dan Perlindungan Sempadan Malaysia (MCBA)": 32,
        "Suruhanjaya Bebas Tatakelakuan Polis (IPCC)": 33
    }

    person_sort_order = 0

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

    def parse_email(self, hidden_email):
        """Parse and decode the email using html unescape"""
        prefix = re.search(r"var prefix = '(.*?)';", hidden_email) #mailto:
        part1 = re.search(r"var addy\w+ = '(.*?)';", hidden_email) #before @
        part2 = re.search(r"addy\w+ = addy\w+ \+ '(.*?)';", hidden_email) # after @

        if part1 and part2:
            part1 = unescape(part1.group(1))
            part2 = unescape(part2.group(1))

            part1_clean = part1.replace("'", "").replace("+", "").strip()
            part2_clean = part2.replace("'", "").replace("+", "").strip()

            email = part1_clean + part2_clean
            email = email.replace(" ","").strip()
        
            if not email.endswith(".gov.my"):
                email = f"{email}.gov.my"

            return email
        return None

    def parse_item(self, response):
        time.sleep(random.uniform(1, 3))  # Delay between 1 to 3 seconds
        page_number = self.extract_page(response.url)
        division = response.css('h2::text').get()

        if division:
            division = division.strip()
        else:
            self.logger.warning(f"Division not found for {response.url}")
            division = "Unknown"  # Fallback for missing divisions

        # Get division_sort_order from the map, fallback to 999 for unknown divisions
        division_sort_order = self.division_sort_map.get(division, 999)

        for item in response.css('ul.category li'):
            self.person_sort_order += 1  # Increment the global person sort order

            person_name = self.extract_name(item.css('div.list-title a::text').get())
            person_position_list = item.css('div.list-title::text').getall()
            person_position = ' '.join([text.strip() for text in person_position_list if text.strip()])
            person_position = re.sub(r'\s+', ' ', person_position).strip()
            person_phone_text = item.css('div.span3::text').get()
            person_phone = self.extract_phone(person_phone_text)

            hidden_email = item.css('script::text').get()
            person_email = self.parse_email(hidden_email) if hidden_email else None

            yield {
                'org_sort': 999,
                'org_id': 'MOHA',
                'org_name': 'KEMENTERIAN DALAM NEGERI',
                'org_type': 'ministry',
                'division_sort': division_sort_order,
                'person_sort_order': self.person_sort_order,
                'division_name': division if division else None,
                'subdivision_name': None,
                'person_name': person_name if person_name else None,
                'position_name': person_position if person_position else None,
                'person_phone': person_phone if person_phone else None,
                'person_email': person_email if person_email else None,
                'person_fax': None,
                'parent_org_id': None,  # is the parent
                #'url': response.url,
                #'page_number': page_number,
            }

    def make_requests_from_url(self, url):
        """Override method to add priority based on division_sort_order"""
        priority = 100  # Default priority for unknown divisions
        division_name = None

        # check if the URL matches the custom division mapping for priority
        for division, sort_order in self.division_sort_map.items():
            if division.lower().replace(" ", "-") in url.lower():
                priority = 100 - sort_order  # higher sort_order -> Lower priority value
                division_name = division
                break

        return scrapy.Request(url, priority=priority, dont_filter=True)
