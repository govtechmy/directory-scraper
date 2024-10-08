import scrapy
from itertools import zip_longest

class MOSTISpider(scrapy.Spider):
    name = "mosti"
    allowed_domains = ["direktori.mosti.gov.my"]
    start_urls = ["https://direktori.mosti.gov.my/directorystaff/list.php"]
    
    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@mosti.gov.my" if (result := condition) else None


    bahagian_mapping = [
        {"directory_url": "?&deptvar=1", "division_name": "Pejabat Menteri"},
        {"directory_url": "?&deptvar=2", "division_name": "Pejabat Timbalan Menteri"},
        {"directory_url": "?&deptvar=3", "division_name": "Pejabat Ketua Setiausaha"},
        {"directory_url": "?&deptvar=4", "division_name": "Pejabat Timbalan Ketua Setiausaha (Pembangunan Teknologi)"},
        {"directory_url": "?&deptvar=5", "division_name": "Pejabat Timbalan Ketua Setiausaha (Perancangan dan Pembudayaan Sains)"},
        {"directory_url": "?&deptvar=8", "division_name": "Pejabat Setiausaha Bahagian Kanan (Pengurusan)"},
        {"directory_url": "?&deptvar=21", "division_name": "Bahagian Dana"},
        {"directory_url": "?&deptvar=22", "division_name": "Bahagian Pemindahan Teknologi dan  Pengkomersialan R&D"},
        {"directory_url": "?&deptvar=23", "division_name": "Bahagian Pembudayaan dan Perkhidmatan STI"},
        {"directory_url": "?&deptvar=24", "division_name": "Bahagian Teknologi Strategik dan Aplikasi S&T"},
        {"directory_url": "?&deptvar=25", "division_name": "Pusat Nanoteknologi Kebangsaan"},
        {"directory_url": "?&deptvar=26", "division_name": "Pejabat Pengurusan Projek Pembangunan Vaksin Malaysia"},
        {"directory_url": "?&deptvar=32", "division_name": "Bahagian Akaun"},
        {"directory_url": "?&deptvar=33", "division_name": "Bahagian Kewangan"},
        {"directory_url": "?&deptvar=34", "division_name": "Bahagian Pembangunan"},
        {"directory_url": "?&deptvar=35", "division_name": "Bahagian Pengurusan Sumber Manusia"},
        {"directory_url": "?&deptvar=36", "division_name": "Bahagian Pengurusan Teknologi Maklumat"},
        {"directory_url": "?&deptvar=37", "division_name": "Bahagian Pentadbiran"},
        {"directory_url": "?&deptvar=39", "division_name": "Bahagian Antarabangsa"},
        {"directory_url": "?&deptvar=41", "division_name": "Bahagian Perancangan Strategik"},
        {"directory_url": "?&deptvar=42", "division_name": "Pusat Maklumat Sains dan Teknologi Malaysia (MASTIC) Maklumat Sains dan Teknologi Malaysia (MASTIC)"},
        {"directory_url": "?&deptvar=43", "division_name": "Bahagian Penguasa Angkasa"},
        {"directory_url": "?&deptvar=44", "division_name": "Unit Audit Dalam"},
        {"directory_url": "?&deptvar=45", "division_name": "Unit Integriti"},
        {"directory_url": "?&deptvar=46", "division_name": "Unit Komunikasi Korporat"},
        {"directory_url": "?&deptvar=47", "division_name": "Unit Perundangan"}
    ]
    
    def start_requests(self):
        for division_sort, row in enumerate(self.bahagian_mapping):
            url = self.start_urls[0]+row["directory_url"]
            yield scrapy.Request(
                url=url,
                callback=self.parse_item,
                meta={
                    "division_sort": division_sort+1,
                    "division_name": row["division_name"]
                }
            )
            
    def parse_item(self, response):
        division_name = response.meta["division_name"]
        division_sort = response.meta["division_sort"]
        unit_name = None
        person_sort = 1
        current_page = response.css("div[class='container']")[-1]

        for unit_name, table in zip_longest(current_page.xpath("//p[text()]/text()").getall(), current_page.xpath("//table[starts-with(@class, 'table')]")):
            for row in table.css("tr"):
                if row.xpath("td[not(*)][1]/text()").get():
                    person_data = {
                        "org_id": "MOSTI",
                        "org_name": "KEMENTERIAN SAINS, TEKNOLOGI DAN INOVASI",
                        "org_sort": 14,
                        "org_type": "ministry",
                        "division_name": division_name,
                        "division_sort": division_sort,
                        "unit_name": unit_name,
                        "person_position": self.none_handler(row.xpath("td[not(*)][3]/text()").get()),
                        "person_name": self.none_handler(row.xpath("td[not(*)][2]/text()").get()),
                        "person_email": self.email_handler(row.xpath("td[not(*)][5]/text()").get()),
                        "person_fax": None,
                        "person_phone": self.none_handler(row.xpath("td[not(*)][4]/text()").get()),
                        "person_sort": person_sort,
                        "parent_prg_id": None
                    }
                    person_sort += 1
                    yield person_data