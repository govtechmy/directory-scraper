import scrapy
import re

class PetraSpider(scrapy.Spider):
    name = 'petra'
    
    base_url ="https://www.petra.gov.my/v2/directory?search_dept=&search_dept={dept}&keyword=&page={page}&per-page=10" 
    
    departments = []
    divisions_name_list = []
    dept_division_map = []
    
    #map units to the correct divisions_name_list (manual work)
    #reference: https://www.petra.gov.my/portal-main/article?id=carta-organisasi
    unit_to_division = {
        'sdp': 'bbe',                  # 'SEKSYEN DASAR DAN PERANCANGAN BEKALAN ELEKTRIK (DPBE)' : 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)'
        'srpi': 'bbe',                 # 'SEKSYEN REGULATORI DAN PEMBANGUNAN INDUSTRI (RPI)' : 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)'
        'upp': 'bbe',                  # 'UNIT PENYELARASAN DAN PENTADBIRAN' : 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)'
        'stbb': 'btl',                 # 'SEKSYEN TENAGA BOLEH BAHARU (TBB)' : 'PEJABAT SETIAUSAHA BAHAGIAN (TENAGA LESTARI)'
        'skt': 'btl',                  # 'SEKSYEN KECEKAPAN TENAGA (KT)' : 'PEJABAT SETIAUSAHA BAHAGIAN (TENAGA LESTARI)'
        'spkp': 'ppkbes',              # 'PEJABAT PENGARAH (SEKSYEN PENYELARASAN DAN KHIDMAT PAKAR)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'uppt': 'ppkbes',              # 'UNIT PENYELARASAN PROJEK & TEKNIKAL' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'uppk': 'ppkbes',              # 'UNIT PEROLEHAN & PENTADBIRAN KONTRAK' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'ks-ppkbes': 'ppkbes',         # 'KHIDMAT SOKONGAN' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'spt-ppkbes': 'ppkbes',        # 'SEKSYEN PROJEK TAPAK (PPKBES SABAH)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'zonbarat-ppkbes': 'ppkbes',   # 'UNIT PROJEK ZON A (BARAT)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'zontengah-ppkbes': 'ppkbes',  # 'UNIT PROJEK ZON B (TENGAH)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'zontimur-ppkbes': 'ppkbes',   # 'UNIT PROJEK ZON C (TIMUR)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'ukub-ppkbes': 'ppkbes',       # 'UNIT KONTRAK DAN UKUR BAHAN' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'dlp-ppkbes': 'ppkbes',        # 'DEFECT LIABILITY PERIOD (DLP)' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'ups-ppkbes': 'ppkbes',        # 'UNIT PENTADBIRAN DAN SOKONGAN' : 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)'
        'spba': 'bap',                 # 'SEKSYEN INDUSTRI PERKHIDMATAN BEKALAN AIR (BA)' : 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN'
        'spp': 'bap',                  # 'SEKSYEN INDUSTRI PERKHIDMATAN PEMBETUNGAN (IP)' : 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN'
        'srpa': 'bap',                 # 'SEKSYEN REGULATORI PERKHIDMATAN AIR (RA)' : 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN'
        'up': 'bap',                   # 'UNIT PENTADBIRAN' : 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN'
        'wst-bpap': 'bap',             # 'UNIT TRANSFORMASI SEKTOR AIR 2040 (WST 2040)' : 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN'
    }

    person_sort_order = 0
    
    def start_requests(self):
        url = "https://www.petra.gov.my/v2/directory"
        # Initial request to scrape department options
        yield scrapy.Request(url=url, callback=self.extract_departments)

    def extract_departments(self, response):
        options = response.xpath("//select[@name='search_dept']/option[not(@disabled)]")
        self.department_division_map = [
            (option.xpath("./@value").get().strip(), option.xpath("normalize-space(text())").get().strip())
            for option in options if option.xpath("./@value").get()
            ]

        self.logger.debug(f"\nExtracted Department-Division Map: \n{self.department_division_map}")

        # Proceed to scrape each department
        for dept, division_name in self.department_division_map:
            division_sort_order = self.department_division_map.index((dept, division_name)) + 1
            url = self.base_url.format(dept=dept, page=1)
            self.logger.debug(f"Starting to scrape department: '{dept}' - '{division_name}'")
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={'dept': dept, 'division_name': division_name, 'page': 1, 'division_sort_order': division_sort_order},
            )

    def parse(self, response):
        dept = response.meta['dept']
        page = response.meta['page']
        division_sort_order = response.meta['division_sort_order']

        # Log which page and dept are being scraped for debugging purposes
        self.logger.debug(f"Scraping department: {dept} on page: {page}")

        rows = response.xpath("//table[@class='table table-bordered table-striped mt-20']/tbody/tr")
        
        items = []
        if not rows:
            self.logger.warning(f"No rows found for department: {dept} on page: {page}")

        for row in rows:
            self.person_sort_order += 1  # Increment person_sort_order for each person

            person_name = row.xpath("./td[2]/text()").get(default='').strip()
            division_or_unit = row.xpath("./td[3]/text()").get(default='').strip()
            person_position = row.xpath("./td[4]/text()").get(default='').strip()
            person_email = row.xpath("./td[5]/text()").get(default='').strip()
            person_phone = row.xpath("./td[6]/text()").get(default='').strip()

            if person_email:
                if "@" in person_email:
                    pass
                else:
                    person_email = f"{person_email}@petra.gov.my"

# ================== Resolving division and unit ==================
            current_division = None
            unit = None

            if division_or_unit:  # If a value for division/unit exists in the table row
                self.logger.debug(f"Processing division_or_unit: {division_or_unit}")
                
                # Step 1: Map full division_or_unit name to a department key (shortcode like 'sdp')
                mapped_dept = next(
                    (dept for dept, division_name in self.department_division_map if division_name == division_or_unit),
                    None
                )
                if mapped_dept:  # If the full name maps to a valid department
                    self.logger.debug(f"Found mapping in department_division_map: {division_or_unit} -> {mapped_dept}")
                    
                    # Step 2: Check if the mapped_dept exists in unit_to_division
                    if mapped_dept in self.unit_to_division:
                        # Resolve the full division name and subdivision name using the mapping
                        current_division = next(
                            (div_name for d, div_name in self.department_division_map if d == self.unit_to_division[mapped_dept]), None)
                        unit = division_or_unit  # Use the full name (e.g., 'SEKSYEN DASAR...')
                        self.logger.debug(f"!!! Resolved unit_to_division mapping: '{mapped_dept}' -> '{unit}'")
                    else:
                        # Log warning for missing mapping
                        self.logger.warning(f"No mapping in unit_to_division for dept: '{mapped_dept}'")
                        current_division = division_or_unit  # Fallback to original division_or_unit name
                        unit = None
                else:
                    # Log warning for no mapping in department_division_map
                    self.logger.warning(f"No mapping in department_division_map for division_or_unit: {division_or_unit}")
                    current_division = division_or_unit  # Fallback to using the original full name
                    unit = None  # No subdivision in this case
            else:
                # No division/unit in the row: assign the default division based on `dept`
                current_division = next(
                    (division_name for d, division_name in self.department_division_map if d == dept),
                    None
                )
                unit = None  # No subdivision in this case
                self.logger.debug(f"No division_or_unit provided. Default division: {current_division}")

# ================== END OF Resolving division and unit ==================

            items.append({
                'org_sort': 999,
                'org_id': "PETRA",
                'org_name': 'KEMENTERIAN PERALIHAN TENAGA DAN TRANSFORMASI AIR',
                'org_type': 'ministry',
                'division_sort': None,  # to later sort based on correct department sequence
                'position_sort_order': self.person_sort_order,
                #'department': dept,
                'division_name': current_division if current_division else None,
                'subdivision_name': unit if unit else None,
                'person_name': person_name if person_name else None,
                'position_name': person_position if person_position else None,
                'person_phone': person_phone if person_phone else None,
                'person_email': person_email if person_email else None,
                'person_fax': None,
                'parent_org_id': None, #is the parent
            })

        # Recalculate and assign correct division_sort for all items on this page
        for item in items:
            if item['division_name']:
                # Match division_name with department_division_map
                division_sort = next(
                    (idx + 1 for idx, (_, div_name) in enumerate(self.department_division_map)
                     if " ".join(div_name.strip().upper().split()) == " ".join(item['division_name'].strip().upper().split())), None)
                item['division_sort'] = division_sort
            yield item

        #pagination (next button)
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(
                url=next_page_url, 
                callback=self.parse, 
                meta={'dept': dept, 'page': page + 1, 'division_sort_order': division_sort_order}
            )
        else:
            self.logger.debug(f"Finished scraping {dept}")
