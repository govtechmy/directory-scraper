import scrapy

class KlnSpider(scrapy.Spider):
    name = 'kln_v9'
    allowed_domains = ['direktori.kln.gov.my']
    start_urls = ['https://direktori.kln.gov.my/?m=wisma+putra&j=Pejabat+Menteri+Luar+Negeri+%28WP1+Aras+3%29'] #Wisma Putra

    def parse(self, response):

        divisions = response.css('select.form-select:nth-of-type(1) option')

        for division in divisions:
            division_name = division.css('::text').get().strip()
            division_value = division.css('::attr(value)').get()
            if division_value:
                division_link = f"https://direktori.kln.gov.my/?m=wisma+putra&j={division_value}"
                yield scrapy.Request(division_link, callback=self.parse_division, meta={'division_name': division_name})

    def parse_division(self, response):
        division_name = response.meta['division_name']

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

            #print(f"Scraped person: {person_name} - {division_name} - {subdivision_name} - {response.url}")

            yield {
                'agency': 'KEMENTERIAN LUAR NEGERI',
                'person_name': person_name,
                'division': division_name,
                'unit': subdivision_name if division_name!=subdivision_name else None,
                'person_position': person_position,
                'person_phone': person_phone,
                'person_email': person_email,
                #'image_url': image_url,
                #'url': response.url
            }


#Wisma Putra (/)
#IDFR, Searcct, Pejabat Perwakilan (X)