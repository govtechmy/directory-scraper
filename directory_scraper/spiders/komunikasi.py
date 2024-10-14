import scrapy
from scrapy_playwright.page import PageMethod

class KOMUNIKASIpider(scrapy.Spider):
    name = "komunikasi"
    allowed_domains = ["komunikasi.gov.my"]
    start_urls = ["https://www.komunikasi.gov.my/hubungi-kami/direktori-kementerian"]

    none_handler = lambda self, condition: result.replace("\xa0", "").strip() if (result := condition) else None
    email_handler = lambda self, condition: result.replace("[@]", "@").replace("[.]", ".").replace("[]", "") if (result := condition) else None

    def start_requests(self):
        for division_order, url in enumerate(self.start_urls):
            yield scrapy.Request(
                url=url,
                callback=self.parse_item,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "div[class='uk-panel']"),
                    ],
                }
            )
    
    async def parse_item(self, response):
        page = response.meta["playwright_page"]
        
        for person_sort, row in enumerate(response.css("article[class='uk-article']").css("div[class='uk-panel']")):
            contact_details = [txt.strip() for txt in row.xpath("div[not(@class)]/text()").getall() if txt.strip()]
            person_data = {
                "org_id": "KOMUNIKASI",
                "org_name": "Kementerian Komunikasi",
                "org_sort": 20,
                "org_type": "ministry",
                "division_name": None,
                "division_sort": 1,
                "subdivision_name": None,
                "position_name": self.none_handler(row.css("div[class='uk-margin']::text").get()),
                "person_name": self.none_handler(row.css("h3 > strong::text").get()),
                "person_email": self.email_handler(self.none_handler(contact_details[1])),
                "person_fax": None,
                "person_phone": self.none_handler(contact_details[0]),
                "position_sort": person_sort+1,
                "parent_org_id": None,
            }
            yield person_data