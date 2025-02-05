import re
import scrapy
from scrapy_playwright.page import PageMethod

class MOESpider(scrapy.Spider):
    name = "moe"
    allowed_domains = ["direktori.moe.gov.my"]
    start_urls = ["https://direktori.moe.gov.my/ajax/public/getdir.php?id=1&textsearch=&selectsearch="]

    none_handler = lambda self, condition: txt_lst[0].strip() if (txt_lst := [datum for datum in result if datum.strip()] if (result := condition) else None) else None
    email_handler = lambda self, condition, email_domain: (result if ("@" in result) else f"{result}{email_domain}") if (result := condition) else None

    division_dict = dict()
    division_mapping = []

    def start_requests(self):
        yield scrapy.Request(
            url="https://direktori.moe.gov.my/",
            callback=self.extract_bahagian,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                        PageMethod("wait_for_selector", "button[type='button']"),
                ],
            }
        )

    def extract_bahagian(self, response):
        href_lst = [
            int(page_id.group()) if (page_id := re.search(r"(\d+$)", txt)) else 0
            for txt in response.css("ul[class='nav nav-stacked col-md-2'][role='tablist'] > li > a::attr(href)").getall()
        ]
        for page_id in href_lst:
            yield scrapy.Request(
                url=f"https://direktori.moe.gov.my/ajax/public/gettab.php?id={page_id}",
                callback=self.extract_mapping,
                meta={"page_id": page_id, "check_length": len(href_lst)}
            )

    def extract_mapping(self, response):
        page_id = response.meta["page_id"]
        check_length = response.meta["check_length"]
        division_code_lst = [
            div_code[0] if (div_code := re.findall(r"(\d+),", string=row)) else None
            for row in response.css(f"div[id='btnlistmain_{page_id}'] > button::attr(onclick)").getall()
        ]
        division_name_lst = [
            div_name
            for idx, div_name in enumerate(response.css(f"div[id='btnlistmain_{page_id}'] > button ::text").getall())
            if idx % 2 == 1
        ]

        self.division_dict[page_id] = [
            {"division_code": div_code, "division": div_name, "page_id": int(page_id)}
            for div_code, div_name in zip(division_code_lst, division_name_lst)
        ]

        if len(self.division_dict) == check_length:
            for page_key in sorted([*self.division_dict.keys()]):
                self.division_mapping.extend(self.division_dict[page_key])

            for division_order, row in enumerate(self.division_mapping):
                url = f"https://direktori.moe.gov.my/ajax/public/getdir.php?id={row['page_id']}&textsearch=&selectsearch={row['division_code']}"
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_item,
                    meta={
                        "playwright": True,
                        "playwright_include_page": True,
                        "playwright_page_methods": [
                            PageMethod("wait_for_selector", "input[type='checkbox']"),
                        ],
                        "division": row["division"],
                        "division_sort_order": division_order+1
                    }
                )

    async def parse_item(self, response):
        page = response.meta["playwright_page"]
        division = response.meta["division"]
        division_sort_order = response.meta["division_sort_order"]
        
        await page.click("input[type='checkbox']")
        # yield {"url": response.url, "body": response.text}
        person_sort_order = 0
        current_unit = None
        current_subunit = None
        for table in response.css("div[class='panel-group'] > div[class='panel panel-default']"):
            current_unit = self.none_handler(table.xpath("div[@class='panel-heading']/text()").getall())
            unit_data = table.xpath("div[starts-with(@class, 'panel-collapse collapse')]/div[@class='panel-body']")
            head_email_domain = self.none_handler(unit_data.xpath("table/thead/tr[1]/th[4]/span/text()").getall())[1:-1]
            for head_row in unit_data.xpath("table/tbody/tr"):
                person_sort_order += 1
                yield {
                    "org_sort": 21,
                    "org_id": "MOE",
                    "org_name": "KEMENTERIAN PENDIDIKAN",
                    "org_type": "ministry",
                    "division_name": division,
                    "division_sort": division_sort_order,
                    "subdivision_name": current_unit,
                    "position_sort": person_sort_order,
                    "person_name": self.none_handler(head_row.xpath("td[2]/text()").getall()),
                    "position_name": self.none_handler(head_row.xpath("td[3]/text()").getall()),
                    "person_phone": self.none_handler(head_row.xpath("td[4]/text()").getall()),
                    "person_email": self.email_handler(head_row.xpath("td[5]/text()").getall(), head_email_domain),
                    "person_fax": None,
                    "parent_org_id": None
                }

            if unit_staff := unit_data.xpath("div//div[@role='tablist']/div[@class='panel panel-default']"):
                for subunit_table in unit_staff:
                    current_subunit = self.none_handler(subunit_table.xpath("div[@class='panel-heading']/h4/a/text()").getall())
                    email_domain = self.none_handler(subunit_table.xpath("div[2]//table/thead/tr[1]/th[4]/span/text()").getall())[1:-1]
                    for subunit_row in subunit_table.xpath("div[2]//table/tbody/tr"):
                        person_sort_order += 1
                        yield {
                            "org_sort": 21,
                            "org_id": "MOE",
                            "org_name": "KEMENTERIAN PENDIDIKAN",
                            "org_type": "ministry",
                            "division_name": division,
                            "division_sort": division_sort_order,
                            "subdivision_name": f"{current_unit} > {current_subunit}",
                            "position_sort": person_sort_order,
                            "person_name": self.none_handler(subunit_row.xpath("td[2]/text()").getall()),
                            "position_name": self.none_handler(subunit_row.xpath("td[3]/text()").getall()),
                            "person_phone": self.email_handler(subunit_row.xpath("td[4]/text()").getall(), email_domain),
                            "person_email": self.none_handler(subunit_row.xpath("td[5]/text()").getall()),
                            "person_fax": None,
                            "parent_org_id": None
                        }

        await page.close()