import re
import scrapy
from scrapy_playwright.page import PageMethod

class MOESpider(scrapy.Spider):
    name = "moe"
    allowed_domains = ["direktori.moe.gov.my"]
    start_urls = ["https://direktori.moe.gov.my/ajax/public/getdir.php?id=1&textsearch=&selectsearch="]

    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: (result if ("@" in result) else f"{result}@moe.gov.my") if (result := condition) else None

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
                if division_order not in [10, 34, 76]:
                    continue
                url = f"https://direktori.moe.gov.my/ajax/public/getdir.php?id={row['page_id']}&textsearch=&selectsearch={row['division_code']}"
                print("#"*30, "\n\n", f"{division_order}\n{row}", "\n\n", "#"*30)
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

        person_sort_order = 1
        current_unit = None
        current_subunit = None
        for table in response.css("div[class='panel panel-default']"):
            if unit_name := table.xpath("div[@class='panel-heading']/text()").getall()[-1].strip():
                current_unit = unit_name
            if table.xpath("div[@class='panel-heading']").attrib.get("href"):
                for top_row in table.css(f"div > div > table[class='table table-striped table-bordered table-hover table-responsive']:nth-child(1)").css("tbody > tr"):
                    if not top_row.css("td:nth-child(2)::text"):
                        continue
                    yield {
                        "org_sort": 21,
                        "org_id": "MOE",
                        "org_name": "KEMENTERIAN PENDIDIKAN",
                        "org_type": "ministry",
                        "division_name": division,
                        "division_sort": division_sort_order,
                        "subdivision_name": current_unit,
                        "position_sort": person_sort_order,
                        "person_name": self.none_handler(top_row.css("td:nth-child(2)::text").get()),
                        "position_name": self.none_handler(top_row.css("td:nth-child(3)::text").get()),
                        "person_phone": self.none_handler(top_row.css("td:nth-child(5)::text").get()),
                        "person_email": self.email_handler(top_row.css("td:nth-child(4)::text").get()),
                        "person_fax": None,
                        "parent_org_id": None
                    }
            else:
                for row in table.css("div[class='row'] > div > div > div[class='panel panel-default']"):
                    if subunit_name := row.css("h4 > a::text").get().strip():
                        current_subunit = subunit_name
                    for data_row in row.css("table > tbody > tr"):
                        yield {
                            "org_sort": 21,
                            "org_id": "MOE",
                            "org_name": "KEMENTERIAN PENDIDIKAN",
                            "org_type": "ministry",
                            "division_name": division,
                            "division_sort": division_sort_order,
                            "subdivision_name": f"{current_unit} > {current_subunit}",
                            "position_sort": person_sort_order,
                            "person_name": self.none_handler(data_row.css("td:nth-child(2)::text").get()),
                            "position_name": self.none_handler(data_row.css("td:nth-child(3)::text").get()),
                            "person_phone": self.none_handler(data_row.css("td:nth-child(5)::text").get()),
                            "person_email": self.email_handler(data_row.css("td:nth-child(4)::text").get()),
                            "person_fax": None,
                            "parent_org_id": None
                        }

        await page.close()