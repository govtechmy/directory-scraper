import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
import re
from urllib.parse import urlparse, parse_qs
from html import unescape  # Import to decode HTML entities

class MohaSpider(CrawlSpider):
    name = 'moha'
    allowed_domains = ['www.moha.gov.my']
    start_urls = ['https://www.moha.gov.my/index.php/ms/kdn1/dir-kdn']

    # custom settings for this spider (moha website will close connection if no delay when scraping)
    custom_settings = {
        'DOWNLOAD_DELAY': 1.5,  # Reduced to 1.5 seconds
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 2,  # Increased start delay to 2 seconds
        'AUTOTHROTTLE_MAX_DELAY': 10,  # Kept the max delay the same
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408],
        'RETRY_WAIT_TIME': 10,
        'DOWNLOAD_TIMEOUT': 30,
        'CONCURRENT_REQUESTS': 4,  # Reduced concurrent requests to 4
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,  # Reduced to 1 per domain
    }

    division_sort_map = {}

    person_sort_order = 0

    rules = (
        Rule(LinkExtractor(allow=r'/index.php/ms/kdn1/dir-kdn$'), callback='extract_bahagian', follow=False),
        Rule(LinkExtractor(allow=r'/index\.php/ms/kdn1/dir-kdn/\d+-[^/]+$'), callback='parse_item', follow=True), # match only division-level pages. (exclude individual-level pages)
        Rule(LinkExtractor(allow=r'/index\.php/ms/kdn1/dir-kdn\?start=\d+'), follow=True),
    )
    
    def extract_bahagian(self, response):
        self.division_sort_map = {
            txt.strip(): idx+1
            for idx, txt
            in enumerate(response.css("div[class='categories-listdirektori'] h3 a::text").getall())
        }

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
       #print("Decoding email using OCR..")
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
        # Removed time.sleep(random.uniform(1, 3)) to rely on Scrapy's built-in delay
        page_number = self.extract_page(response.url)
        division = response.css('h2::text').get()

        if division:
            division = division.strip()
            # print(f"Division found: {division} for {response.url}")
        else:
            self.logger.warning(f"Division not found for {response.url}")
            division = "Lain-Lain"  # Fallback for missing divisions

        # Get division_sort_order from the map, fallback to 999 for unknown divisions
        division_sort_order = self.division_sort_map.get(division, 999)

        for item in response.css('ul.category li'):
            self.person_sort_order += 1  # Increment the global person sort order

            person_name = self.extract_name(item.css('div.list-title a::text').get())
            person_position_list = item.css('div.list-title::text').getall()
            person_position = ' '.join([text.strip() for text in person_position_list if text.strip()])
            person_position = re.sub(r'\s+', ' ', person_position).strip()
            if person_position.startswith('|'):
                person_position = person_position[1:].strip()
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
                'position_sort_order': self.person_sort_order,
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