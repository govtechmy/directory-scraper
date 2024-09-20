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

    rules = (
        Rule(LinkExtractor(allow=r"/\d+?"), callback='parse_item'),
    )

    def parse_item(self, response):
        print(response.url)
        clean_html = lambda string: re.sub(r"<.+?>|[\r\t\n]{2,}", "", string)
        extract_number = lambda string: re.findall(pattern=r"03[\d\- ]+\d", string=string)
        division = [text.strip() for line in response.css("div[class='card-body'] > div > div[class='col'] > b::text").getall() for text in line.split("\n") if text][0]
        email_extension = clean_html(response.css("thead > tr > td > b").getall()[2]).split()[1]
        department_number = extract_number(clean_html(response.css("div[class='alert alert-warning']").getall()[0]))
        default_unit = None

        for row_idx, row in enumerate(response.css("tbody > tr")):
            person_data = {"agency": "Kementerian Pendidikan Tinggi", "division": division, "unit": default_unit}
            for idx, col in enumerate(row.css("td")):
                if unit_name := col.css("b").getall():
                    default_unit = clean_html(unit_name[0])
                    continue

                temp_col = [i for i in clean_html(col.getall()[0]).split("\n") if len(i) > 2]
                if idx == 1:
                    # print(col)
                    person_data.update({
                        "person_name": temp_col.pop(0) if temp_col else None,
                        "person_position": temp_col.pop(0) if temp_col else None,
                        "position_grade": temp_col.pop(0) if temp_col else None
                    })
                if idx == 2:
                    person_data.update({
                        "person_email": f"{temp_col.pop(0) if temp_col else None}{email_extension}"
                    })
                if idx == 3:
                    if department_number:
                        person_data.update({
                            "person_phone": f"{department_number[0]} ext {temp_col.pop(0) if temp_col else None}" 
                        })
                    else:
                        person_data.update({
                            "person_phone": f"{temp_col.pop(0) if temp_col else None}" 
                        })
            if len(person_data) > 2:
                yield person_data