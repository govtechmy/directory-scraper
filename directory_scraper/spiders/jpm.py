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
        'https://direktori.jpm.gov.my/jpm/7', # to fix
        'https://direktori.jpm.gov.my/jpm/8', # to fix: got section/unit structure
        'https://direktori.jpm.gov.my/jpm/9', # to fix: missing data; for section & unit structure
        'https://direktori.jpm.gov.my/jpm/10',
        'https://direktori.jpm.gov.my/jpm/11', # to fix: missing data; for section & unit structure
        'https://direktori.jpm.gov.my/jpm/12', # to fix: missing data; for section & unit structure
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

            main_unit_case1 = section.css('div.card-header h2::text').get(default='').strip()
            main_unit_case2 = section.css('div.card-header h2 b::text').get(default='').strip() #jpm11

            detailed_unit_case1 = section.css('div.card-header h3::text').get(default='').strip() #jpm5, jpm8, jpm2
            detailed_unit_case2 = section.css('div.card-header2 h4 b::text').get(default='').strip()

            if main_unit_case1:
                main_unit = main_unit_case1
            else:
                main_unit = main_unit_case2

            if detailed_unit_case1:
                detailed_unit = detailed_unit_case1
            else:
                detailed_unit = detailed_unit_case2

            if not main_unit and detailed_unit:
                main_unit = detailed_unit
                detailed_unit = None

            #iterate contact row in the section's table
            for contact in section.css('tbody tr.d-flex'):
                person_name = contact.css('td.col-4 b::text').get(default='').strip()
                person_position = contact.css('td.col-3::text').get(default='').strip()
                person_phone_prefix = contact.css('td.col-2::text').get(default='').strip()
                person_phone = f"03{person_phone_prefix}" if person_phone_prefix else ''
                
                email_list = contact.css('td.col-3::text').getall() 
                if email_list:
                    person_email_prefix = email_list[-1].strip()
                else:
                    person_email_prefix = ''     
                person_email = f"{person_email_prefix}@jpm.gov.my" if person_email_prefix else ''

                # Handle cases where there might be only main_unit, only detailed_unit, both, or neither
                unit_full = None
                if main_unit and detailed_unit:
                    unit_full = f"{main_unit} > {detailed_unit}"  # Combine main unit and detailed unit
                elif main_unit:
                    unit_full = main_unit  # Only main unit exists

                yield {
                    'agency': agency,
                    'division': division if division else None,
                    #'division_address': division_address,
                    #'division_phone': division_phone,
                    'person_name': person_name if person_name else None,
                    'unit': unit_full if unit_full else None,
                    'person_phone': person_phone if person_phone else None,
                    'person_position': person_position if person_position else None,
                    'person_email': person_email if person_email else None,
                    'url': response.url,
                }
