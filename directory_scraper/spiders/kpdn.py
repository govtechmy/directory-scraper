import re
from typing import Iterable
import scrapy

class KPDNSpider(scrapy.Spider):
    name = "kpdn"
    
    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@kpdn.gov.my" if (result := re.sub(r"/txt2img/", "", condition)) else None
    
    start_urls = [
        "https://insid.kpdn.gov.my/direktori/bahagian/92",
        "https://insid.kpdn.gov.my/direktori/bahagian/91",
        "https://insid.kpdn.gov.my/direktori/bahagian/78",
        "https://insid.kpdn.gov.my/direktori/bahagian/111",
        "https://insid.kpdn.gov.my/direktori/bahagian/125",
        "https://insid.kpdn.gov.my/direktori/bahagian/44",
        "https://insid.kpdn.gov.my/direktori/bahagian/50",
        "https://insid.kpdn.gov.my/direktori/bahagian/121",
        "https://insid.kpdn.gov.my/direktori/bahagian/18",
        "https://insid.kpdn.gov.my/direktori/bahagian/99",
        "https://insid.kpdn.gov.my/direktori/bahagian/113",
        "https://insid.kpdn.gov.my/direktori/bahagian/20",
        "https://insid.kpdn.gov.my/direktori/bahagian/94",
        "https://insid.kpdn.gov.my/direktori/bahagian/124",
        "https://insid.kpdn.gov.my/direktori/bahagian/54",
        "https://insid.kpdn.gov.my/direktori/bahagian/139",
        "https://insid.kpdn.gov.my/direktori/bahagian/98",
        "https://insid.kpdn.gov.my/direktori/bahagian/112",
        "https://insid.kpdn.gov.my/direktori/bahagian/100",
        "https://insid.kpdn.gov.my/direktori/bahagian/110",
        "https://insid.kpdn.gov.my/direktori/bahagian/38",
        "https://insid.kpdn.gov.my/direktori/bahagian/23",
        "https://insid.kpdn.gov.my/direktori/bahagian/79",
        "https://insid.kpdn.gov.my/direktori/bahagian/1",
        "https://insid.kpdn.gov.my/direktori/bahagian/76",
        "https://insid.kpdn.gov.my/direktori/bahagian/74",
        "https://insid.kpdn.gov.my/direktori/bahagian/75",
        "https://insid.kpdn.gov.my/direktori/bahagian/140",
        "https://insid.kpdn.gov.my/direktori/bahagian/142",
        "https://insid.kpdn.gov.my/direktori/bahagian/141",
        "https://insid.kpdn.gov.my/direktori/bahagian/143",
        "https://insid.kpdn.gov.my/direktori/negeri/1",
        "https://insid.kpdn.gov.my/direktori/negeri/2",
        "https://insid.kpdn.gov.my/direktori/negeri/3",
        "https://insid.kpdn.gov.my/direktori/negeri/4",
        "https://insid.kpdn.gov.my/direktori/negeri/5",
        "https://insid.kpdn.gov.my/direktori/negeri/6",
        "https://insid.kpdn.gov.my/direktori/negeri/7",
        "https://insid.kpdn.gov.my/direktori/negeri/8",
        "https://insid.kpdn.gov.my/direktori/negeri/9",
        "https://insid.kpdn.gov.my/direktori/negeri/10",
        "https://insid.kpdn.gov.my/direktori/negeri/11",
        "https://insid.kpdn.gov.my/direktori/negeri/12",
        "https://insid.kpdn.gov.my/direktori/negeri/13",
        "https://insid.kpdn.gov.my/direktori/negeri/14",
        "https://insid.kpdn.gov.my/direktori/negeri/15",
        "https://insid.kpdn.gov.my/direktori/negeri/16"
    ]
    
    def start_requests(self):
        for url in self.start_urls:
            print(url)
            yield scrapy.Request(url=url, callback=self.parse)
    
    def parse(self, response):
        division_sort_order = self.start_urls.index(response.url)
        for row in response.css("tbody > tr"):
            division = self.none_handler(response.xpath("//h4/text()").getall()[-1])
            unit = row.css("td:nth-child(7)").css("::text").get().replace("-", "")
            subunit = row.css("td:nth-child(6)").css("::text").get().replace("-", "")

            if unit and subunit:
                unit_name = f"{unit} > {subunit}"
            elif unit and not subunit:
                unit_name = unit
            elif not unit and subunit:
                unit_name = subunit
            else:
                unit_name = None

            person_data = {
                "org_id": "KPDN",
                "org_name": "KEMENTERIAN PERDAGANGAN DALAM NEGERI DAN KOS SARA HIDUP",
                "org_sort": 25,
                "org_type": "ministry",
                "division_name": division,
                "division_sort": division_sort_order,
                "subdivision_name": unit_name,
                "position_name": self.none_handler(row.css("td:nth-child(4)").css("::text").get()),
                "person_name": re.sub(r"[\n ]{2,}", " ",self.none_handler(row.css("td:nth-child(2)").css("::text").get())),
                "person_email": self.email_handler(row.css("td:nth-child(3)").css("img::attr(src)").get()),
                "person_fax": None,
                "person_phone": self.none_handler(row.css("td:nth-child(5)").css("::text").get()),
                "person_sort": self.none_handler(row.css("td:nth-child(1)").css("::text").get()),
                "parent_org_id": None
            }
            yield person_data