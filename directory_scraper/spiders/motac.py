import re
import json
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class MOTACSpider(CrawlSpider):
    name = "motac"
    allowed_domains = ["www.motac.gov.my"]
    start_urls = ["https://www.motac.gov.my/direktori"]

    rules = (
        Rule(LinkExtractor(allow="direktori"), callback='parse_item'),
    )
    
    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@motac.gov.my" if (result := condition) else None
    clean_list = lambda self, lst: [elem.strip() for elem in lst if elem.strip()]
    
    url_lst = [
        "https://www.motac.gov.my/direktori/menteri",
        "https://www.motac.gov.my/direktori/timbalan-menteri",
        "https://www.motac.gov.my/direktori/ksu",
        "https://www.motac.gov.my/direktori/tksupl",
        "https://www.motac.gov.my/direktori/tksuk",
        "https://www.motac.gov.my/direktori/pejabat-timbalan-ketua-setiausaha-pengurusan",
        "https://www.motac.gov.my/direktori/dpl",
        "https://www.motac.gov.my/direktori/dk",
        "https://www.motac.gov.my/direktori/hak",
        "https://www.motac.gov.my/direktori/pi",
        "https://www.motac.gov.my/direktori/pa",
        "https://www.motac.gov.my/direktori/kewangan",
        "https://www.motac.gov.my/direktori/pentadbiran",
        "https://www.motac.gov.my/direktori/akaun",
        "https://www.motac.gov.my/direktori/bppp",
        "https://www.motac.gov.my/direktori/bpm",
        "https://www.motac.gov.my/direktori/psm",
        "https://www.motac.gov.my/direktori/pp",
        "https://www.motac.gov.my/direktori/matic",
        "https://www.motac.gov.my/direktori/ukk",
        "https://www.motac.gov.my/direktori/puu",
        "https://www.motac.gov.my/direktori/uad",
        "https://www.motac.gov.my/direktori/ui",
        "https://www.motac.gov.my/direktori/kpi",
        "https://www.motac.gov.my/direktori/vm2026",
        "https://www.motac.gov.my/direktori/mm2h",
        "https://www.motac.gov.my/direktori/motac-perlis",
        "https://www.motac.gov.my/direktori/motac-kedah",
        "https://www.motac.gov.my/direktori/motac-penang",
        "https://www.motac.gov.my/direktori/motac-perak",
        "https://www.motac.gov.my/direktori/motac-selangor",
        "https://www.motac.gov.my/direktori/motac-melaka",
        "https://www.motac.gov.my/direktori/motac-negeri-sembilan",
        "https://www.motac.gov.my/direktori/motac-johor",
        "https://www.motac.gov.my/direktori/motac-kelantan",
        "https://www.motac.gov.my/direktori/motac-terengganu",
        "https://www.motac.gov.my/direktori/motac-pahang",
        "https://www.motac.gov.my/direktori/motac-sarawak",
        "https://www.motac.gov.my/direktori/motac-sabah",
        "https://www.motac.gov.my/direktori/motac-kl-putrajaya",
        "https://www.motac.gov.my/direktori/motac-labuan"
    ]

    def parse_item(self, response):
        division = response.css("h1::text").get()
        
        for sort_order, row in enumerate(response.css("tbody > tr")):
            contact_details = {
                data_type: text
                for data_type, text in zip(row.xpath("td[2]/i/@class"), self.clean_list(row.xpath("td[2]/text()")))
            }
            person_data = {
                "org_id": "MOTAC",
                "org_name": "KEMENTERIAN PELANCONGAN, SENI DAN BUDAYA",
                "org_sort": 19,
                "org_type": "ministry",
                "division_name": division,
                "division_sort": self.url_lst.index(response.url)+1,
                "subdivision_name": self.none_handler(row.xpath("td[1]/small/text()").get()),
                "position_name": self.none_handler(row.xpath("td[1]/text()[2]").get()),
                "person_name": self.none_handler(row.xpath("td[1]/strong/text()").get()),
                "person_email": contact_details.get("uk-icon-envelope-square"),
                "person_fax": None,
                "person_phone": contact_details.get("uk-icon-phone-square"),
                "person_sort": sort_order+1,
                "parent_org_id": None,
            }
            yield person_data