import scrapy
import re

class KPKTSpider(scrapy.Spider):
    name = "kpkt"
    start_urls = ['https://edirektori.kpkt.gov.my/edirektori/']

    def parse(self, response):
        #follow the url links using regex for the 'grid' pattern
        grid_links = response.css('a::attr(href)').re(r'https://edirektori.kpkt.gov.my/edirektori/index.php/home/grid/\d+')

        #exclude these Jabatan for now. (bcs the structure of their division & unit is different than KPKT)
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
            yield scrapy.Request(
                url=link,
                callback=self.parse_grid_page,
                meta={'grid_url': link}  # pass the grid URL to be used later
            )


    def parse_grid_page(self, response):
        # Extract the main_division from the static HTML page
        main_division = response.css('#detailjab strong::text').get()
        if main_division:
            main_division = main_division.strip()
        else:
            self.logger.warning(f'Main division not found on {response.url}, setting it as "Unknown"')
            main_division = "Unknown"

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
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36',
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

        #loop thru all division and unit panels
        division_panels = response.css('.panel.panel-primary')

        for division_panel in division_panels:
            division = division_panel.css('h4.panel-title a::text').get().strip()
            
            unit_panels = division_panel.css('.panel-success')

            for unit_panel in unit_panels:
                unit = unit_panel.css('h5.panel-title a::text').get().strip()
                
                #set unit to None if unit==division. redundency
                #if division == main_division:
                #    division = None

                rows = unit_panel.css('table tbody tr')
                for row in rows:
                    person_name_raw = row.css('td:nth-child(3)').xpath('normalize-space(strong/text())').get()
                    person_name = person_name_raw.replace("\n", "").strip() if person_name_raw else None
                    person_position_raw = row.css('td:nth-child(3)').xpath('normalize-space(text()[following-sibling::br])').get()
                    person_position = person_position_raw.strip() if person_position_raw else None
                    person_phone = row.css('td:nth-child(6)::text').get()
                    person_fax = row.css('td:nth-child(7)::text').get()

                    yield {
                        'agency': "KEMENTERIAN PERUMAHAN DAN KERAJAAN TEMPATAN",
                        #'main_division': main_division,
                        'person_name': person_name,
                        'division': main_division,
                        'unit': f"{division} > {unit}" if division and (main_division != unit) else None,  # combine division + unit_detailed as one string.(only if 'division' exists, & division != unit)
                        #'unit_detailed': unit,
                        'person_position': person_position,
                        'person_phone': person_phone,
                        'person_email': None, #email is stored as image. to solve later.
                        #'person_fax': person_fax,
                        #'url': grid_url  
                    }