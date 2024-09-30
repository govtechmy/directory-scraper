import scrapy
import re

class KPKTSpider(scrapy.Spider):
    name = "kpkt"
    start_urls = ['https://edirektori.kpkt.gov.my/edirektori/']

    person_sort_order = 0  #init
    division_sort = {}  #dictionary to store division_name sort order based on "grid number"

    def parse(self, response):
        #follow the url links using regex for the 'grid' pattern
        grid_links = response.css('a::attr(href)').re(r'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/\d+')

        #exclude these Jabatan for now. (bcs the structure of their division_name & unit_name is different than KPKT)
        excluded_links = [
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/25',
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/26',
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/27',
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/28',
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/29',
            'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/30'
            ]
        
        filtered_grid_links = [link for link in grid_links if link not in excluded_links]

        #loop thru each extracted grid link & visit each grid page to extract main_division
        for link in filtered_grid_links:
            grid_number = int(re.search(r'/grid/(\d+)', link).group(1))  #extract the grid number. to set as value for priority
            yield scrapy.Request(
                url=link,
                callback=self.parse_grid_page,
                meta={'grid_url': link, 'grid_number': grid_number},  # pass the grid URL to be used later
                priority=grid_number
            )

    def parse_grid_page(self, response):
        # Extract the main_division from the static HTML page
        main_division = response.css('#detailjab strong::text').get()
        if main_division:
            main_division = main_division.strip()
        else:
            self.logger.warning(f'Main division_name not found on {response.url}, setting it as "Unknown"')
            main_division = "Unknown"

        grid_number = response.meta['grid_number']
        if main_division not in self.division_sort:
            self.division_sort[main_division] = grid_number #division_sort=grid number

        grid_url = response.meta['grid_url']
        grid_id = re.search(r'/grid/(\d+)', grid_url).group(1)

        form_data = {
            'id': grid_id
        }

        #send the AJAX request
        yield scrapy.FormRequest(
            url='https://edirektori.kpkt.gov.my/edirektori/index.php/home/ajx_dbah/',
            formdata=form_data,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0 Safari/537.36',
                'Accept': 'text/html, */*; q=0.01',
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'X-Requested-With': 'XMLHttpRequest',
                'Origin': 'https://edirektori.kpkt.gov.my',
                'Referer': f'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/{grid_id}',
            },
            callback=self.parse_ajx_response,
            meta={'grid_id': grid_id, 'grid_url': grid_url, 'main_division': main_division}  #to later pass main_division to the callback
        )

    def parse_ajx_response(self, response):
        main_division = response.meta['main_division']
        grid_url = response.meta['grid_url']

        #loop thru all division_name and unit_name panels
        division_panels = response.css('.panel.panel-primary')

        for division_panel in division_panels:
            division_name = division_panel.css('h4.panel-title a::text').get().strip()
            
            unit_panels = division_panel.css('.panel-success')

            for unit_panel in unit_panels:
                unit_name = unit_panel.css('h5.panel-title a::text').get().strip()

                #set unit_name to None if unit_name==division_name. redundency
                #if division_name == main_division:
                #    division_name = None

                rows = unit_panel.css('table tbody tr')
                for row in rows:
                    person_name_raw = row.css('td:nth-child(3)').xpath('normalize-space(strong/text())').get()
                    person_name = person_name_raw.replace("\n", "").strip() if person_name_raw else None
                    person_position_raw = row.css('td:nth-child(3)').xpath('normalize-space(text()[preceding-sibling::strong/following-sibling::br[1]])').get()
                    person_position = person_position_raw.strip() if person_position_raw else None
                    person_phone = row.css('td:nth-child(6)::text').get()
                    person_fax = row.css('td:nth-child(7)::text').get()

                    self.person_sort_order += 1 #increment globally

                    yield {
                        'org_sort': 4,
                        'org_id': 'KPKT',
                        'org_name': 'KEMENTERIAN PERUMAHAN DAN KERAJAAN TEMPATAN',
                        'org_type': 'ministry',
                        'division_sort': self.division_sort[main_division],
                        'person_sort_order': self.person_sort_order,
                        #'main_division': main_division,
                        'division_name': main_division,
                        'unit_name': f"{division_name} > {unit_name}" if division_name and (main_division != unit_name) else None,  # combine division_name + unit_detailed as one string.(only if 'division_name' exists, & division_name != unit_name)
                        'person_position': person_position,
                        'person_name': person_name,
                        'person_phone': person_phone,
                        'person_email': None,  #email is stored as image. to solve later.
                        'person_fax': person_fax,
                        'parent_org_id': [], #is the parent
                        #'url': grid_url 
                    }