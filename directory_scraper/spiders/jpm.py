import scrapy

class JPMSpider(scrapy.Spider):
    name = 'jpm'
    start_urls = [
                    'https://direktori.jpm.gov.my/jpm/1',
                   'https://direktori.jpm.gov.my/jpm/2',
                   'https://direktori.jpm.gov.my/jpm/3',
                   'https://direktori.jpm.gov.my/jpm/4',
                   'https://direktori.jpm.gov.my/jpm/5',
                   'https://direktori.jpm.gov.my/jpm/6',
                #   'https://direktori.jpm.gov.my/jpm/7', # to fix
                #   'https://direktori.jpm.gov.my/jpm/8', # to fix: got section/unit structure
                #   'https://direktori.jpm.gov.my/jpm/9', # to fix: missing data; for section & unit structure
                   'https://direktori.jpm.gov.my/jpm/10',
                #   'https://direktori.jpm.gov.my/jpm/11', # to fix: missing data; for section & unit structure
                #   'https://direktori.jpm.gov.my/jpm/12', # to fix: missing data; for section & unit structure
                   'https://direktori.jpm.gov.my/jpm/13'
                   ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_static
            )

    def parse_static(self, response):
        agency = "JABATAN PERDANA MENTERI"

        division = response.css('h3.card-title::text').get(default='').strip()
        division_address = response.css('span.text-sm.text.mb-0::text').get(default='').strip()
        division_phone = response.css('p.mt-1 span.text-default::text').get(default='').strip()

        #extract each accordion section
        accordion_sections = response.css('div.card')

        for section in accordion_sections:
            # Extract unit title (e.g., "PEJABAT TIMBALAN KETUA SETIAUSAHA KANAN")
            unit = section.css('div.card-header h3::text').get(default='').strip()

            #iterate contact row in the section's table
            for contact in section.css('tbody tr.d-flex'):
                person_name = contact.css('td.col-4 b::text').get(default='').strip()
                person_position = contact.css('td.col-3::text').get(default='').strip()
                person_phone_prefix = contact.css('td.col-2::text').get(default='').strip()
                person_phone = f"03{person_phone_prefix}" if person_phone_prefix else ''
                person_email_prefix = contact.css('td.col-3::text').getall()[-1].strip()
                person_email = f"{person_email_prefix}@jpm.gov.my" if person_email_prefix else ''

                yield {
                    'agency': agency,
                    'division': division,
                    #'division_address': division_address,
                    #'division_phone': division_phone,
                    'person_name': person_name,
                    'unit': unit,
                    'person_phone': person_phone,
                    'person_position': person_position,
                    'person_email': person_email,
                    }
