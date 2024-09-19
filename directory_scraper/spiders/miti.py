import scrapy

class MitiSpider(scrapy.Spider):
    name = 'miti'
    allowed_domains = ['www.miti.gov.my']
    start_urls = ['https://www.miti.gov.my/index.php/edirectory/edirectory_list/4']

    def start_requests(self):
        #1: switch language to BM
        yield scrapy.Request(
            url='https://www.miti.gov.my/index.php/multilingual/switch_language/BM', #can switch to EN
            callback=self.after_language_switch
        )

    def after_language_switch(self, response):
        #2: then, proceed to scrape the directory page
        yield scrapy.Request(
            url='https://www.miti.gov.my/index.php/edirectory/edirectory_list/4',
            callback=self.parse
        )

    def parse(self, response):
        csrf_token = response.css('input[name="ci_csrf_token"]::attr(value)').get() #csrf_token is in static html

        payload = {
            'ci_csrf_token': csrf_token,
            'name': '',
            'division': '0',
            'section': '0',
            'edirectory_group': '4',
            'keyword': 'All'
        }

        # Send a POST request with the form data
        yield scrapy.FormRequest(
            url='https://www.miti.gov.my/index.php/edirectory/filter_search',
            formdata=payload,
            headers={
                'Accept': '*/*',
                'Accept-Encoding': 'gzip, deflate, br, zstd',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Origin': 'https://www.miti.gov.my',
                'Referer': 'https://www.miti.gov.my/index.php/edirectory/edirectory_list/4',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest'
            },
            callback=self.parse_results,
        )

    def parse_results(self, response):
        person_items = response.css('div.list-item')

        for item in person_items:
            person_name = item.css('h1 a::text').get(default='').strip()
            person_position = item.css('p::text').get(default='').strip()
            profile_url = item.css('h1 a::attr(href)').get(default='')
            image_url = response.urljoin(item.css('img::attr(src)').get(default=''))

            details = item.css('table.profile-detail-table tbody tr') #table
            person_phone = self.get_data_label(details, 0)
            person_email = self.get_data_label(details, 1)
            division = self.get_data_label(details, 2)
            section = self.get_data_label(details, 3)

            if person_email:
                person_email = person_email + '@miti.gov.my'

            yield {
                'agency': "KEMENTERIAN PELABURAN, PERDAGANGAN DAN INDUSTRI MALAYSIA",
                'person_name': person_name,
                'division': division,
                'unit': section,
                'person_position': person_position,
                'person_phone': person_phone,
                'person_email': person_email,
                #'image_url': image_url,
                #'profile_url': profile_url,
            }

    def get_data_label(self, details, index):
        """Helper function: To extract the data-label value & handle missing data gracefully (e.g return None if no "section" data)."""
        if index < len(details):
            return details[index].css('td.data-label::text').get(default='').strip()
        return None

#language EN/BM
