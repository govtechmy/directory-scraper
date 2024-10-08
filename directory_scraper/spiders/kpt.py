import re
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
    
    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    position_handler = lambda self, condition: result.replace("•", "").strip() if (result := condition) else None
    sort_handler = lambda self, condition: result.strip()[:-1] if (result := condition) else None

    def parse_item(self, response):

        current_unit = None
        division = self.none_handler(response.css("div[class='col'] > b::text").get())
        department_number = " ".join(response.css("div[class='alert alert-warning']::text").getall())
        department_number = number[0] if (number := re.findall(r"0[\d /-]+", department_number)) else None
        email_extension = re.findall(r"@.*", response.xpath("//thead/tr/td[3]/b/text()").get())[0]
        email_handler = lambda condition: f"{result}{email_extension}" if (result := condition) else None

        for row in response.css("tbody > tr"):
            if row.attrib:
                continue
            elif unit := row.css("b::text").get():
                current_unit = unit
            else:
                phone_extension = self.none_handler(row.xpath("td[4]/text()").get())
                if re.match(r'\d{4}', phone_extension) and department_number:
                    phone_number = f"{department_number} ext {phone_extension}"
                else:
                    phone_number = phone_extension
                person_data = {
                    "org_id": "KPT",
                    "org_name": "KEMENTERIAN PENDIDIKAN TINGGI",
                    "org_sort": 18, 
                    "org_type": "ministry",
                    "division_name": division,
                    "division_sort": self.directory_urls.index(response.url)+1,
                    "unit_name": current_unit,
                    "person_position": self.position_handler(row.xpath("td[2]/font/text()").get()),
                    "person_name": self.none_handler(row.xpath("td[2]/text()").get()),
                    "person_email": email_handler(row.xpath("td[3]/text()").get()),
                    "person_fax": None,
                    "person_phone": phone_number,
                    "person_sort": self.sort_handler(row.xpath("td[1]/text()").get()),
                    "parent_org_id": None
                }
                yield person_data