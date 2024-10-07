import re
import json
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class KPTSpider(CrawlSpider):
    name = "kpt"
    allowed_domains = ["app.mohe.gov.my"]
    start_urls = [
    "https://app.mohe.gov.my/direktori/menteri/KPT",
    "https://app.mohe.gov.my/direktori/jabatan/KPT",
    "https://app.mohe.gov.my/direktori/jabatan/JPT",
    "https://app.mohe.gov.my/direktori/jabatan/JPPKK",
    "https://app.mohe.gov.my/direktori/jabatan/AKEPT/161"
    ]
    
    directory_urls = [
        "https://app.mohe.gov.my/direktori/menteri/KPT/68",
        "https://app.mohe.gov.my/direktori/menteri/KPT/74",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/67",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/73",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/72",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/63",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/80",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/60",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/62",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/164",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/165",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/59",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/61",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/58",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/66",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/69",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/75",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/81",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/152",
        "https://app.mohe.gov.my/direktori/jabatan/KPT/167",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/89",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/90",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/91",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/98",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/102",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/100",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/101",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/93",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/95",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/96",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/94",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/103",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/99",
        "https://app.mohe.gov.my/direktori/jabatan/JPT/92",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/82",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/16",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/83",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/5",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/154",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/3",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/155",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/8",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/6",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/14",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/10",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/13",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/148",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/147",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/76",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/149",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/2",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK/160",
        "https://app.mohe.gov.my/direktori/jabatan/AKEPT/161"
    ]

    rules = (
        Rule(LinkExtractor(allow=r"/\d+?"), callback='parse_item'),
    )

    def parse_item(self, response):
        clean_html = lambda string: re.sub(r"<.+?>|[\r\t\n]{2,}", "", string)
        extract_number = lambda string: re.findall(pattern=r"03[\d\- ]+\d", string=string)
        division = [text.strip() for line in response.css("div[class='card-body'] > div > div[class='col'] > b::text").getall() for text in line.split("\n") if text][0]
        email_extension = clean_html(response.css("thead > tr > td > b").getall()[2]).split()[1]
        department_number = extract_number(clean_html(response.css("div[class='alert alert-warning']").get()[0]))
        default_unit = None

        for row_idx, row in enumerate(response.css("tbody > tr")):
            person_data = {
                "org_id": "KPT",
                "org_name": "KEMENTERIAN PENDIDIKAN TINGGI",
                "org_sort": 18,
                "division_name": division,
                "unit_name": default_unit,
                "division_sort": self.directory_urls.index(response.url)+1
            }
            for idx, col in enumerate(row.css("td")):
                if col.attrib.get("colspan") and col.attrib.get("valign"):
                    default_unit = clean_html(col.css("b::text").get())
                    print(default_unit)
                    continue

                temp_col = [i for i in clean_html(col.getall()[0]).split("\n") if len(i) > 1]
                if idx == 0:
                    person_data.update({
                        "person_sort_order": temp_col.pop(0)[:-1] if temp_col else None
                    })
                elif idx == 1:
                    # print(col)
                    person_data.update({
                        "person_name": temp_col.pop(0) if temp_col else None,
                    })
                elif idx == 2:
                    person_data.update({
                        "person_email": f"{temp_col.pop(0) if temp_col else None}{email_extension}"
                    })
                elif idx == 3:
                    if department_number:
                        extension_number = (f" ext {temp_col.pop(0).replace('*', '')}" if temp_col else "")
                        phone_number = department_number + (extension_number or "")
                        person_data.update({
                            "person_phone": phone_number
                        })
                    else:
                        person_data.update({
                            "person_phone": f"{temp_col.pop(0) if temp_col else None}" 
                        })
            if len(person_data) > 5:
                yield {
                    "org_id": person_data.get("org_id"),
                    "org_name": person_data.get("org_name"),
                    "org_sort": person_data.get("org_sort"), 
                    "org_type": "ministry",
                    "division_name": person_data.get("division_name"),
                    "division_sort": person_data.get("division_sort"),
                    "unit_name": person_data.get("unit_name"),
                    "person_position": person_data.get("person_sort_order"),
                    "person_name": person_data.get("person_name"),
                    "person_email": person_data.get("person_email"),
                    "person_fax": None,
                    "person_phone": person_data.get("person_phone"),
                    "person_sort": person_data.get("person_sort_order"),
                    "parent_org_id": None
                }