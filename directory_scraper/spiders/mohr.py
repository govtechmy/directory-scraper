from typing import Iterable
import scrapy
from scrapy_playwright.page import PageMethod

class MOHRScraper(scrapy.Spider):
    name = "mohr"
    
    custom_settings = {
        "COOKIES_ENABLED": True,
        "DOWNLOAD_DELAY": 5
    }

    start_urls = ["https://app1.mohr.gov.my/staff/staff_name.php?department="]
    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@mohr.gov.my" if (result := condition) else None

    
    division_mapping = {
        "00010": "PEJABAT MENTERI ",
        "00020": "PEJABAT TIMBALAN MENTERI ",
        "00030": "PEJABAT KETUA SETIAUSAHA ",
        "00040": "PEJABAT TIMBALAN KETUA SETIAUSAHA (DASAR DAN ANTARABANGSA) ",
        "00041": "BAHAGIAN DASAR ",
        "00041-A": "CAWANGAN DASAR SUMBER MANUSIA ",
        "00041-B": "CAWANGAN DASAR PERBURUHAN ",
        "00041-C": "CAWANGAN PERANCANGAN STRATEGIK, PARLIMEN DAN KABINET ",
        "00042": "ILMIA (INSTITUTE OF LABOUR MARKET INFORMATION AND ANALYSIS) ",
        "00043": "BAHAGIAN ANTARABANGSA ",
        "00044": "MAJLIS PERUNDINGAN GAJI NEGARA ",
        "00045": "BAHAGIAN PENGURUSAN PEKERJA ASING ",
        "00050": "PEJABAT TIMBALAN KETUA SETIAUSAHA (OPERASI) ",
        "00051": "BAHAGIAN PEMBANGUNAN, KEWANGAN & SUMBER MANUSIA ",
        "00051-A": "CAWANGAN PENGURUSAN SUMBER MANUSIA ",
        "00051-B": "CAWANGAN PEMBANGUNAN DAN KEWANGAN ",
        "00052": "BAHAGIAN KHIDMAT PENGURUSAN ",
        "00053": "BAHAGIAN AKAUN ",
        "00054": "BAHAGIAN PENGURUSAN MAKLUMAT ",
        "00055": "CAWANGAN KAWALSELIA DAN PENGUATKUASAAN ",
        "00060": "BAHAGIAN UNDANG-UNDANG ",
        "00070": "UNIT AUDIT DALAM ",
        "00080": "UNIT KOMUNIKASI KORPORAT ",
        "00100": "UNIT INTEGRITI ",
        "00110": "UNIT KOORDINASI PENDIDIKAN DAN LATIHAN TEKNIKAL DAN VOKASIONAL (TVET) ",
        "10000": "JABATAN TENAGA KERJA SEMENANJUNG ",
        "20000": "JABATAN TENAGA KERJA SABAH ",
        "30000": "JABATAN TENAGA KERJA SARAWAK ",
        "40000": "JABATAN HAL EHWAL KESATUAN SEKERJA ",
        "50000": "JABATAN PERHUBUNGAN PERUSAHAAN ",
        "60000": "JABATAN PEMBANGUNAN KEMAHIRAN ",
        "70000": "MAHKAMAH PERUSAHAAN ",
        "80000": "JABATAN KESELAMATAN DAN KESIHATAN PEKERJAAN ",
        "90000": "JABATAN TENAGA MANUSIA "
    }
    
    def start_requests(self):
        for division_sort, (code, name) in enumerate(self.division_mapping.items()):
            yield scrapy.Request(
                    url=f"https://app1.mohr.gov.my/staff/staff_name.php?department={code}",
                    callback=self.parse,
                    meta={
                        "code": code,
                        "name": name,
                        "division_sort": division_sort
                    }
                )
    
    def parse(self, response):
        with open("/Users/mydigital/Desktop/KD Work/directory-scraper/a.html", "w") as f:
            f.write(response.text)
        code = response.meta["code"]
        name = response.meta["name"]
        division_sort = response.meta["division_sort"]

        if code in ["00041-A", "00041-B", "00041-C"]:
            current_division = "BAHAGIAN DASAR"
            current_unit = name
        elif code in ["00051-A", "00051-B", "00055"]:
            current_division = "BAHAGIAN PEMBANGUNAN, KEWANGAN & SUMBER MANUSIA"
            current_unit = name
        else:
            current_division = name
            current_unit = None

        for person_sort, row in enumerate(response.css("tbody > tr")):
            person_data = {
                "org_id": "MORH",
                "org_name": "KEMENTERIAN SUMBER MANUSIA",
                "org_sort": 29,
                "org_type": "ministry",
                "division_name": current_division,
                "division_sort": division_sort+1,
                "unit_name": current_unit,
                "person_position": self.none_handler(row.xpath("td[2]/text()").get()),
                "person_name": self.none_handler(row.xpath("td[1]/text()").get()),
                "person_email": self.email_handler(row.xpath("td[5]/a/text()").get()),
                "person_fax": None,
                "person_phone": self.none_handler(row.xpath("td[5]/text()").get()),
                "person_sort": person_sort+1,
                "parent_org_id": None,
            }
            yield(person_data)