import scrapy

class KLNSpider(scrapy.Spider):
    name = 'kln'
    allowed_domains = ['direktori.kln.gov.my']

    person_sort_order = 0 #init
    division_sort_order = 0 #init

    division_tracker = {} #dictionary to track divisions and their assigned sort order

    start_urls = [
        'https://direktori.kln.gov.my/?m=wisma+putra&j=Pejabat+Menteri+Luar+Negeri+%28WP1+Aras+3%29', #Wisma Putra
        #'https://direktori.kln.gov.my/?m=idfr&j=Institut+Diplomasi+dan+Hubungan+Luar+Negeri', #IDFR
        #'https://direktori.kln.gov.my/?m=searcct&j=Pusat+Serantau+Asia+Tenggara+Bagi+Mencegah+Keganasan', #SEARCCT
        #'https://direktori.kln.gov.my/?m=pejabat+perwakilan&p=Abu+Dhabi' #Pejabat Perwakilan
    ]

    def parse(self, response):
        #print(f"Spider started. Current URL: {response.url}")

        #check if the start URL is "idfr" or "searcct" - these url don't require division links (the url is sufficient for scraping all persons without needing to "follow" division links)
        if 'idfr' in response.url or 'searcct' in response.url:
            division_name = response.url.split('j=')[1].replace('+', ' ').strip()
            #print(f"Direct parsing for: {division_name}")
            yield from self.parse_person_info(response, division_name)  #directly parse the person information to parse_person_info() (parse_division() is not needed)

        #handle "pejabat perwakilan" (follow division links)
        elif 'pejabat+perwakilan' in response.url:
            divisions = response.css('select.form-select:nth-of-type(1) option')

            for division in divisions:
                division_name = division.css('::text').get().strip()
                division_value = division.css('::attr(value)').get()

                if division_value and division_value != "":
                    division_link = f"https://direktori.kln.gov.my/?m=pejabat+perwakilan&p={division_value}"
                    print(f"Following division: {division_name}, Link: {division_link}")

                    if division_name not in self.division_tracker: #assign division_sort_order based on appearance order (as seen on the html page)

                        self.division_sort_order += 1
                        self.division_tracker[division_name] = self.division_sort_order

                    priority_value = -self.division_tracker[division_name] #negative to let lower sort to have higher priority


                    yield scrapy.Request(
                        division_link, 
                        callback=self.parse_division, 
                        meta={'division_name': division_name}, 
                        priority=priority_value  #set priority based on division_sort_order (to control the scraping priority)
                    )

        #handle "wisma putra" (follow division links)
        elif 'wisma+putra' in response.url:
            divisions = response.css('select.form-select:nth-of-type(1) option')

            for division in divisions:
                division_name = division.css('::text').get().strip()
                division_value = division.css('::attr(value)').get()

                if division_value:
                    division_link = f"https://direktori.kln.gov.my/?m=wisma+putra&j={division_value}"

                    if division_name not in self.division_tracker: #assign division_sort_order based on appearance order (as seen on the html page)
                        self.division_sort_order += 1
                        self.division_tracker[division_name] = self.division_sort_order

                    priority_value = -self.division_tracker[division_name] ##negative to let lower sort to have higher priority

                    yield scrapy.Request(
                        division_link, 
                        callback=self.parse_division, 
                        meta={'division_name': division_name}, 
                        priority=priority_value  #set priority based on division_sort_order (to control the scraping priority)
                    )

    def parse_division(self, response):
        division_name = response.meta['division_name']
        #print(f"\n\nScraping division: {division_name}, URL: {response.url}")

        subdivision_blocks = response.css('ul.comments-list')

        for subdivision_block in subdivision_blocks:
            subdivision_name = subdivision_block.css('h5.comments-title::text').get('').strip()

            yield from self.scrape_person_info(subdivision_block, division_name, subdivision_name, response)

    def scrape_person_info(self, block, division_name, subdivision_name, response):
        person_entries = block.css('li.comment')

        for person in person_entries:
            person_name = person.css('a.author-name::text').get('').strip()
            person_position = person.css('span.date::text').get('').strip()

            person_email = person.xpath('.//i[@class="fa fa-envelope"]/following-sibling::text()').get('').strip().lstrip(':').strip()
            person_phone = person.xpath('.//i[@class="fa fa-phone"]/following-sibling::text()').get('').strip().lstrip(':').strip()

            image_url = person.css('.comment-avatar img::attr(src)').get()
            if image_url:
                image_url = response.urljoin(image_url)

            #increment person_sort_order for each person, prioritizing divisions with lower division_sort_order
            self.person_sort_order += 1

            yield {
                'org_sort': 999,
                'org_id': 'KLN',
                'org_name': 'KEMENTERIAN LUAR NEGERI',
                'org_type': 'ministry',
                'division_sort': self.division_tracker[division_name],
                'position_sort': self.person_sort_order,
                'division_name': division_name if division_name else None,
                'subdivision_name': subdivision_name if division_name != subdivision_name else None, 
                'person_name': person_name if person_name else None,
                'position_name': person_position if person_position else None,
                'person_phone': person_phone if person_phone else None,
                'person_email': person_email if person_email else None,
                'person_fax': None,
                'parent_org_id': None, #is the parent
                #'image_url': image_url if image_url else None,
                #'url': response.url
            }

    def parse_person_info(self, response, division_name):
        """Directly scrape person info when the 'start URL' is sufficient to get all persons"""

        subdivision_blocks = response.css('ul.comments-list')

        for subdivision_block in subdivision_blocks:
            subdivision_name = subdivision_block.css('h5.comments-title::text').get('').strip()
            person_entries = subdivision_block.css('li.comment')

            for person in person_entries:
                person_name = person.css('a.author-name::text').get('').strip()
                person_position = person.css('span.date::text').get('').strip()

                person_email = person.xpath('.//i[@class="fa fa-envelope"]/following-sibling::text()').get('').strip().lstrip(':').strip()
                person_phone = person.xpath('.//i[@class="fa fa-phone"]/following-sibling::text()').get('').strip().lstrip(':').strip()

                image_url = person.css('.comment-avatar img::attr(src)').get()
                if image_url:
                    image_url = response.urljoin(image_url)

                self.person_sort_order += 1

                yield {
                    'agency_id': 'KLN',
                    'agency': 'KEMENTERIAN LUAR NEGERI',
                    'division_sort_order': self.division_tracker[division_name],
                    'person_sort_order': self.person_sort_order,
                    'person_name': person_name if person_name else None,
                    'division': division_name if division_name else None,
                    'unit': subdivision_name if division_name != subdivision_name else None,
                    'person_position': person_position if person_position else None,
                    'person_phone': person_phone if person_phone else None,
                    'person_email': person_email if person_email else None,
                    #'image_url': image_url if image_url else None,
                    #'url': response.url
                }

#Wisma Putra, IDFR, Searcct, Pejabat Perwakilan (/)
