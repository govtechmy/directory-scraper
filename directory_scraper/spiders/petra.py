import scrapy
import re

class PetraSpider(scrapy.Spider):
    name = 'petra'
    
    base_url = "https://www.petra.gov.my/portal-main/directory?frontendpage=directory&params=&search_dept={dept}&keyword=&page={page}&per-page=10"

    #list of departments to loop through
    departments = [
        'ment', 'timb', 'ksu', 'uuu', 'bpsha', 'ukk', 'uad', 'ui', 'tksutas', 
        'bbe', 'sdp', 'srpi', 'upp', 'btl', 'stbb', 'skt', 'ppkbes', 'spkp', 'uppt', 
        'uppk', 'ks-ppkbes', 'spt-ppkbes', 'zonbarat-ppkbes', 'zontengah-ppkbes', 
        'zontimur-ppkbes', 'ukub-ppkbes', 'dlp-ppkbes', 'ups-ppkbes', 'AAIBE', 
        'tksuap', 'bap', 'spba', 'spp', 'srpa', 'up', 'wst-bpap', 'bsah', 'pengurusan', 
        'spsm', 'bkew', 'bp', 'akaun', 'bpm', 'skp',
        #'air', 'tenaga', #empty tables
    ]

    #map units to the correct divisions (manual work)
    #reference: https://www.petra.gov.my/portal-main/article?id=carta-organisasi
    unit_to_division = {
        'sdp': 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)',
        'srpi': 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)',
        'upp': 'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)',
        'stbb': 'PEJABAT SETIAUSAHA BAHAGIAN (TENAGA LESTARI)',
        'skt': 'PEJABAT SETIAUSAHA BAHAGIAN (TENAGA LESTARI)',
        'spkp': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'uppt': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'uppk': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'ks-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'spt-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'zonbarat-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'zontengah-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'zontimur-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'ukub-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'dlp-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'ups-ppkbes': 'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'spba': 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
        'spp': 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
        'srpa': 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
        'up': 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
        'wst-bpap': 'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
    }

    #list of divisions for direct mapping
    divisions = [
        'PEJABAT MENTERI',
        'PEJABAT TIMBALAN MENTERI', 
        'PEJABAT KETUA SETIAUSAHA',
        'PEJABAT PENASIHAT UNDANG-UNDANG (PUU)',
        'BAHAGIAN PEMBANGUNAN (BP)',
        'BAHAGIAN AKAUN (BA)',
        'BAHAGIAN PERANCANGAN STRATEGIK DAN HUBUNGAN ANTARABANGSA (BPSA)',
        'UNIT KOMUNIKASI KORPORAT (UKK)',
        'UNIT AUDIT DALAM (UAD)',
        'UNIT INTEGRITI (UI)',
        'PEJABAT TIMBALAN KETUA SETIAUSAHA (TENAGA)', 
        'PEJABAT SETIAUSAHA BAHAGIAN (BEKALAN ELEKTRIK)',
        'PEJABAT SETIAUSAHA BAHAGIAN (TENAGA LESTARI)',
        'PASUKAN PROJEK KHAS BEKALAN ELEKTRIK SABAH (PPKBES)',
        'UNIT AKAUN AMANAH INDUSTRI BEKALAN ELEKTRIK (AAIBE)',
        'PEJABAT TIMBALAN KETUA SETIAUSAHA (AIR)',
        'BAHAGIAN PERKHIDMATAN AIR DAN PEMBETUNGAN',
        'BAHAGIAN SUMBER AIR (BSA)',
        'PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN)',
        'BAHAGIAN PENGURUSAN SUMBER MANUSIA (BPSM)',
        'BAHAGIAN KEWANGAN DAN PEROLEHAN (BKEW)',
        'BAHAGIAN PENGURUSAN MAKLUMAT (BPM)',
        'BAHAGIAN KHIDMAT PENGURUSAN (BKP)'
    ]

    def start_requests(self):
        for dept in self.departments:
            url = self.base_url.format(dept=dept, page=1)
            self.logger.info(f"Starting to scrape {dept}")
            yield scrapy.Request(url=url, callback=self.parse, meta={'dept': dept, 'page': 1})

    def parse(self, response):
        dept = response.meta['dept']
        page = response.meta['page']
        
        # Log which page and dept are being scraped for debugging purposes
        self.logger.info(f"Scraping department: {dept} on page: {page}")

        rows = response.xpath("//table[@class='table table-bordered table-striped mt-20']/tbody/tr")

        if not rows:
            self.logger.warning(f"No rows found for department: {dept} on page: {page}")

        for row in rows:
            person_name = row.xpath("./td[2]/text()").get(default='').strip()
            division_or_unit = row.xpath("./td[3]/text()").get(default='').strip()
            person_position = row.xpath("./td[4]/text()").get(default='').strip()
            person_email_prefix = row.xpath("./td[5]/text()").get(default='').strip()
            person_phone = row.xpath("./td[6]/text()").get(default='').strip()
            person_email = f"{person_email_prefix}@petra.gov.my" if person_email_prefix else None

            current_division = None
            unit = None

            #map based on dept value (use unit_to_division mapping)
            if dept in self.unit_to_division:
                current_division = self.unit_to_division[dept]
                unit = division_or_unit  # The value in the table is the unit
            elif dept in self.departments:
                current_division = next((d for d in self.divisions if dept in d.lower()), None)
                if not current_division:
                    current_division = division_or_unit  # Fallback to using the table value as the division

            yield {
                'agency': 'KEMENTERIAN PERALIHAN TENAGA DAN TRANSFORMASI AIR',
                'department': dept,
                'person_name': person_name,
                'division': current_division,
                'unit': unit,
                'person_position': person_position,
                'person_phone': person_phone,
                'person_email': person_email,
            }

        #pagination (next button)
        next_page = response.xpath("//li[@class='next']/a/@href").get()
        if next_page:
            next_page_url = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_url, callback=self.parse, meta={'dept': dept, 'page': page + 1})
        else:
            self.logger.info(f"Finished scraping {dept}")
