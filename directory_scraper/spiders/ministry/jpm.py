import scrapy
import re

class JPMSpider(scrapy.Spider):
    name = 'jpm'
    start_urls = ['https://direktori.jpm.gov.my']

    custom_settings = {
        'RETRY_TIMES': 2,                # Retry failed requests 2x
        'CLOSESPIDER_TIMEOUT': 300,      # Stop the spider after 5 mins
    }

    person_sort_order = 0 

    def parse(self, response):
        dynamic_urls = response.css('a.list-group-item::attr(href)').getall()

        # print(f"Extracted URLs: {dynamic_urls}")

        for i, url in enumerate(dynamic_urls):
            full_url = response.urljoin(url)
            division_sort = i + 1
            priority_value = len(dynamic_urls) - i  #set higher priority for earlier URLs
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_static,
                errback=self.handle_error,
                meta={'division_sort': division_sort},  # Pass division_sort to the callback
                priority=priority_value  #set priority for ordering
            )

    def handle_error(self, failure):
        self.logger.error(f"Request failed for {failure.request.url}: {repr(failure)}")

    def parse_static(self, response):
        division_sort = response.meta['division_sort']
        division_name = response.css('h3.card-title::text').get(default='').strip()
        division_address = response.css('span.text-sm.text.mb-0::text').get(default='').strip()
        division_phone = response.css('p.mt-1 span.text-default::text').get(default='').strip()

        #extract each accordion section
        accordion_sections = response.css('div.card')

        for section in accordion_sections:

            main_unit_case1 = section.css('div.card-header h2::text').get(default='').strip()  # most jpm
            main_unit_case2 = section.css('div.card-header h2 b::text').get(default='').strip()  # jpm11

            detailed_unit_case1 = section.css('div.card-header h3::text').get(default='').strip()  # jpm5, jpm8, jpm2, and most jpm
            detailed_unit_case2 = section.css('div.card-header2 h4 ::text').get(default='').strip()  # jpm11

            if main_unit_case1:
                main_unit = main_unit_case1
            elif main_unit_case2:
                main_unit = main_unit_case2
            else:
                main_unit = None

            if detailed_unit_case1:
                detailed_unit = detailed_unit_case1
            elif detailed_unit_case2:
                detailed_unit = detailed_unit_case2
            else:
                detailed_unit = None

            if not main_unit and detailed_unit:
                main_unit = detailed_unit
                detailed_unit = None

            # Handle cases where there might be only main_unit, only detailed_unit, both, or neither
            unit_full = None
            if main_unit and detailed_unit:  # if both main unit_name and detailed unit_name exist
                unit_full = f"{main_unit} > {detailed_unit}"  # Combine main unit_name and detailed unit_name
            elif detailed_unit:
                unit_full = detailed_unit  # if only detailed_unit exists
            elif main_unit:
                unit_full = main_unit  # if only main_unit exists

            #iterate contact row in the section's table
            for contact in section.css('tbody tr'):
                self.person_sort_order += 1  # Increment global person_sort_order

                person_name = contact.css('td.col-4 b::text').get(default='').strip()
                person_position = contact.css('td.col-3::text').get(default='').strip()
                person_phone_prefix = contact.css('td.col-2::text').get(default='').strip()

                if person_phone_prefix:
                    person_phone = person_phone_prefix if person_phone_prefix.startswith("03") or person_phone_prefix.startswith("01") else f"03-{person_phone_prefix}"
                else:
                    person_phone = ''
                
                person_email_prefix = contact.css('td.col-3 canvas.email::attr(data-email)').get(default='').strip()
                person_email = f"{person_email_prefix}@jpm.gov.my" if person_email_prefix else None

                yield {
                    'org_sort': 1,
                    'org_id': 'JPM',
                    'org_name': 'JABATAN PERDANA MENTERI',
                    'org_type': 'ministry',
                    'division_sort': division_sort,
                    'position_sort_order': self.person_sort_order,
                    'division_name': division_name if division_name else None,
                    #'division_address': division_address,
                    #'division_phone': division_phone,
                    'subdivision_name': unit_full if unit_full else None,
                    #'main_unit': main_unit, #debugging
                    #'detailed_unit': detailed_unit, #debugging
                    'person_name': person_name if person_name else None,
                    'position_name': person_position if person_position else None,
                    'person_phone': person_phone if person_phone else None,
                    'person_email': person_email if person_email else None,
                    'person_fax': None,
                    'parent_org_id': None, #is the parent
                    #'url': response.url,
                }
