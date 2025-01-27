import re
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class KPTSpider(CrawlSpider):
    name = "kpt"
    allowed_domains = ["app.mohe.gov.my"]
    start_urls=[
        "https://app.mohe.gov.my/direktori/menteri/KPT",
        "https://app.mohe.gov.my/direktori/jabatan/KPT",
        "https://app.mohe.gov.my/direktori/jabatan/JPT",
        "https://app.mohe.gov.my/direktori/jabatan/JPPKK",
        "https://app.mohe.gov.my/direktori/jabatan/AKEPT",
    ]

    sort_pages = {
       "https://app.mohe.gov.my/direktori/jabatan/AKEPT": {
            "Akademi Kepimpinan Pendidikan Tinggi": {"page_sort": "1", "division_sort": 0},
            "length": 1
        }
    }

    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    position_handler = lambda self, condition: result.replace("•", "").strip() if (result := condition) else None
    name_handler = lambda self, txt_lst: result[0] if (result := [txt.strip() for txt in txt_lst if txt.strip()]) else None
    sort_handler = lambda self, condition: result.strip()[:-1] if (result := condition) else None

    overall_count = 0

    rules = (
        Rule(LinkExtractor(allow=r"/{1}[A-Z]+$"), callback="parse_sort_order"),
        Rule(LinkExtractor(allow=r"/{1}\d+$"), callback="parse_item"),
    )

    def parse_sort_order(self, response):
        sort_rows = response.xpath("//tbody/tr/td//text()").getall()
        if sort_rows:
            order_dict = {response.url: {name.strip(): {"page_sort": page_sort.strip(), "division_sort": 0} for (page_sort, name) in zip(sort_rows[::2], sort_rows[1::2])}}
            order_dict[response.url]["length"] = len(order_dict)
            self.sort_pages.update(order_dict)
        
        for page_url in self.start_urls:
            data = self.sort_pages[page_url]
            page_count = 0
            for division in data.keys():
                if division != "length":
                    data[division]["division_sort"] = self.overall_count + int(data[division]["page_sort"])
                    page_count += 1
            count += page_count

    def parse_item(self, response):
        current_unit = None
        parent_url = response.url.rsplit("/", maxsplit=1)[0]
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
                    "org_sort": 18, 
                    "org_id": "KPT",
                    "org_name": "KEMENTERIAN PENDIDIKAN TINGGI",
                    "org_type": "ministry",
                    "division_sort": self.sort_pages[parent_url][division]["division_sort"],
                    "division_name": division,
                    "subdivision_name": current_unit,
                    "position_sort": self.sort_handler(row.xpath("td[1]/text()").get()),
                    "person_name": self.name_handler(row.xpath("td[2]/text()").getall()),
                    "position_name": self.position_handler(row.xpath("td[2]/font/text()").get()),
                    "person_phone": phone_number,
                    "person_email": email_handler(row.xpath("td[3]/text()").get()),
                    "person_fax": None,
                    "parent_org_id": None
                }
                yield person_data