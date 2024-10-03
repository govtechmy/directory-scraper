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
        phone_regex = re.compile(r"03[\d\- ]+\d")

        directory_data = [
            [data for data in re.split(r"[\r\t\n]{2,}", re.sub(r"<.+?>", "", person_data))[1:-1] if data]
            for person_data in response.css("td[class='uk-width-7-10']").getall()
        ]

        for person_order, datapoint in enumerate(directory_data):
            name = datapoint[0].strip()
            position = datapoint[1].strip()
            unit = datapoint[2].strip() if len(datapoint) > 3 else None
            email = email_str.strip() if (email_str := re.sub(pattern=phone_regex, repl="", string=datapoint[-1])) else None
            phone = phone_str[0].strip() if (phone_str := re.findall(pattern=phone_regex, string=datapoint[-1])) else None

            person_data = {
                "org_id": "MOTAC",
                "org_name": "KEMENTERIAN PELANCONGAN, SENI DAN BUDAYA",
                "org_sort": 19,
                "org_type": "ministry",
                "division_name": division,
                "division_sort": self.url_lst.index(response.url)+1,
                "unit_name": unit,
                "person_position": position,
                "person_name": name,
                "person_email": email+"@motac.gov.my",
                "person_fax": "NULL",
                "person_phone": phone,
                "person_sort": person_order+1,
                "parent_org_id": "NULL"
            }

            yield person_data