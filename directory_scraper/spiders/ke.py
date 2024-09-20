import scrapy
import json
import re

class KESpider(scrapy.Spider):
    name = 'ke'
    allowed_domains = ['ekonomi.gov.my']
    start_urls = ['https://www.ekonomi.gov.my/ms/profil-jabatan/organisasi/direktori']

    def parse(self, response):
        last_page_link = response.css('a[rel="last"]::attr(href)').get()
        if last_page_link:
            total_pages = int(re.search(r'page=(\d+)', last_page_link).group(1)) #find the last page
        else:
            total_pages = 0 #set 0

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
            'pager_element': '0', #'page' parameter is to be updated when looping, since we need to increment the page number
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

        for page in range(total_pages + 1):  #increment +1 the page number until the last page. loop from 0-last page
            payload = base_payload.copy()
            payload['page'] = str(page)
            
            yield scrapy.FormRequest(
                url=ajax_url,
                method='POST',
                formdata=payload,
                headers=headers,
                callback=self.parse_ajax_response,
                meta={'page': page} #update 'page' parameter
            )

    def parse_ajax_response(self, response):
        json_data = json.loads(response.text)
        
        #find the "data" section where the HTML is inserted
        for item in json_data:
            if item.get('command') == 'insert' and item.get('data'):
                html_content = scrapy.Selector(text=item['data'])
                
                #extract table rows
                rows = html_content.css('table tbody tr')
                
                for row in rows:
                    person_name = row.css('td:nth-child(2) p::text').get('').strip()
                    person_position = row.css('td:nth-child(2) p::text').getall()[-1].strip()
                    division = row.css('td:nth-child(3)::text').get('').strip()
                    person_email = row.css('td:nth-child(4)::text').get('').strip() + '@ekonomi.gov.my'
                    person_phone = row.css('td:nth-child(5)::text').get('').strip()

                    yield {
                        'person_name': person_name,
                        'person_position': person_position,
                        'division': division,
                        'person_email': person_email,
                        'person_phone': person_phone
                    }

        #print(f"Processed page {response.meta['page']}")