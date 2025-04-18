import scrapy
import json
import re

class EKONOMISpider(scrapy.Spider):
    name = 'ekonomi'
    allowed_domains = ['ekonomi.gov.my']
    start_urls = ['https://www.ekonomi.gov.my/ms/profil-jabatan/organisasi/direktori']

    person_sort_order = 0 #init
    division_sort = 0 #init

    #Dictionary to track divisions and their assigned sort order
    division_tracker = {}

    #Do manual mapping for "unit_name" key. To map "unit_name" with its relevant "division_name"
    division_unit_mapping = {
        'Seksyen Perakaunan Pengurusan': 'Bahagian Akaun',
        'Seksyen Pemantauan dan Perundingan': 'Bahagian Akaun',
        'Seksyen Perakaunan Kewangan': 'Bahagian Akaun',
        'Seksyen Pembangunan Sumber Manusia (Latihan)': 'Bahagian Sumber Manusia',
        'Seksyen Pengurusan Sumber Manusia (Perkhidmatan)': 'Bahagian Sumber Manusia'
    }

    def parse(self, response):
        last_page_link = response.css('a[rel="last"]::attr(href)').get()
        if last_page_link:
            total_pages = int(re.search(r'page=(\d+)', last_page_link).group(1))  # Find the last page
        else:
            total_pages = 0  # Set 0 if no pagination

        #AJAX endpoint where the data is loaded dynamically via POST
        ajax_url = 'https://www.ekonomi.gov.my/ms/views/ajax?_wrapper_format=drupal_ajax'

        base_payload = {
            '_wrapper_format': 'drupal_ajax',
            'view_name': 'directory',
            'view_display_id': 'block_1',
            'view_args': '',
            'view_path': '/node/14',
            'view_base_path': '',
            'view_dom_id': 'ad0c89cf4789151f31ca8a9cc808abadeee0d85d05109cf1ead1d8efe5a86cb3',
            'pager_element': '0',  # 'page' parameter is to be updated when looping, since we need to increment the page number
            '_drupal_ajax': '1',
            'ajax_page_state[theme]': 'mea',
            'ajax_page_state[theme_token]': '',
            'ajax_page_state[libraries]': 'addtoany/addtoany.front,better_exposed_filters/general,bootstrap/dropdown,bootstrap/popover,bootstrap/tooltip,core/drupal.dialog.ajax,filter/caption,flickity/local,flickity/settings,google_analytics/google_analytics,lazy/lazy,poll/drupal.poll-links,printfriendly/printfriendly-libraries,statistics/drupal.statistics,system/base,views/views.ajax,views/views.module,we_megamenu/form.we-mega-menu-frontend'
        }

        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.ekonomi.gov.my/ms/profil-jabatan/organisasi/direktori',
        }

        for page in range(total_pages + 1):  #increment +1 the page number until the last page, loop from 0-last page
            payload = base_payload.copy()
            payload['page'] = str(page)

            yield scrapy.FormRequest(
                url=ajax_url,
                method='POST',
                formdata=payload,
                headers=headers,
                callback=self.parse_ajax_response,
                meta={'page': page},  #update 'page' parameter
                priority=-page  #priority(earlier pages have higher priority)
            )

    def parse_ajax_response(self, response):
        json_data = json.loads(response.text)
        
        #find the "data" section where the HTML is inserted
        for item in json_data:
            if item.get('command') == 'insert' and item.get('data'):
                html_content = scrapy.Selector(text=item['data'])
                
                rows = html_content.css('table tbody tr')
                
                for row in rows:
                    person_name = row.css('td:nth-child(2) p::text').get('').strip()
                    person_position = row.css('td:nth-child(2) p::text').getall()[-1].strip()
                    division_name = row.css('td:nth-child(3)::text').get('').strip()
                    person_email = row.css('td:nth-child(4)::text').get('').strip()
                    person_phone = row.css('td:nth-child(5)::text').get('').strip()

                    # if person_email:
                    #     if "@" in person_email:
                    #         pass
                    #     else:
                    #         person_email = f"{person_email}@ekonomi.gov.my"

                    if person_email:
                        person_email = person_email.replace('[at]', '@').replace('[dot]', '.').replace('[.]', '.').replace('[@]', '@')
                        person_email = person_email.replace('[com]', '.com').replace('[my]', '.my')

                        person_email = person_email.strip().replace(" ", "")

                        if person_email.startswith('@'):
                            person_email = None
                        elif "@" in person_email and "." in person_email.split("@")[-1]:
                            pass
                        elif "julianaj@jkr" in person_email: #JULIANA BINTI JAAFAR
                            pass
                        else:
                            person_email = f"{person_email}@ekonomi.gov.my"

                    #increment person_sort_order for each person
                    self.person_sort_order += 1

                    #check if the division_name is in the 'division_unit_mapping'
                    if division_name in self.division_unit_mapping:
                        unit_name = division_name  #set the current division_name as the "unit_name"
                        division_name = self.division_unit_mapping[division_name]  #then, map the correct "division_name"
                    else:
                        unit_name = None

                    #track by division_name only, and ignore unit_name in this case
                    if division_name not in self.division_tracker:
                        #if it's a new division_name, increment the division_sort and store it
                        self.division_sort += 1
                        self.division_tracker[division_name] = self.division_sort

                    yield {
                        'org_sort': 2,
                        'org_id': 'EKONOMI',
                        'org_name': 'KEMENTERIAN EKONOMI',
                        'org_type': 'ministry',
                        'division_sort': self.division_tracker[division_name],
                        'position_sort_order': self.person_sort_order,
                        'division_name': division_name if division_name else None,
                        'subdivision_name': unit_name if unit_name else None,
                        'person_name': person_name if person_name else None,
                        'position_name': person_position if person_position else None,
                        'person_email': person_email if person_email else None,
                        'person_phone': person_phone if person_phone else None,
                        'person_fax': None,
                        'parent_org_id': None, #is the parent
                        #'page': response.meta['page']
                    }
