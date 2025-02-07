import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod
from time import sleep

class MOHSpider(scrapy.Spider):
    name = "moh"
    allowed_domains = ["www.moh.gov.my"]
    start_urls = ["https://www.moh.gov.my/index.php/edirectory/member_list"]
    
    none_handler = lambda self, condition: result if (result := condition) else None
    
    division_mapping = []

    def start_requests(self):
        yield scrapy.Request(
            url="https://www.moh.gov.my/index.php/edirectory/edirectory_list/1?mid=92",
            callback=self.extract_bahagian,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "select[name='division'][id='division-search']"),
                ]
            }
        )

    def extract_bahagian(self, response):
        select_dict = {
            row.css("::text").get(): division_code
            for row in response.xpath("//select[@name='division']/option")
            if (division_code := int(row.attrib.get("value")))
        }
        self.division_mapping = [
            {
                "division_name": data.css("a::text").get(),
                "division_code": data.css("::attr(href)").get().rsplit("/", maxsplit=2)[-2],
                "division_sort_order": idx+1
            }
            for idx, data in enumerate(response.css("div[id^='tabs'] > div[class='scrollbox lvl-content']"))
            if (href.endswith("/1") if (href := data.css("a::attr(href)").get()) else False)
        ]
        
        for row in self.division_mapping:
            code = row["division_code"]
            url = f"{self.start_urls[0]}/{code}/1"
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "a[class^='paginate_button next']"),
                    ],
                    "division_sort_order": row["division_sort_order"],
                    "division_name": row["division_name"],
                    "page_number": 0
                },
            )
    
    async def parse(self, response):
        page = response.meta["playwright_page"]
        division_sort_order = response.meta["division_sort_order"]
        page_number = 1
        total_pages = int(pagenum) if (pagenum := response.xpath("//a[starts-with(@class, 'paginate_button') and @tabindex=0 and not(contains(text(), 'Next'))][last()]/text()").get()) else 0
        while page_number <= total_pages:
            current_page = Selector(text=await page.content())
            page_number = int(current_page.xpath("//a[@class='paginate_button current']/text()").get())

            for person_sort, data_point in enumerate(current_page.css("div[class='profile-detail col-8-12']")):
                name = self.none_handler(data_point.css("a::text").get())
                position = self.none_handler(data_point.css("p::text").get())
                phone = self.none_handler(data_point.css("tbody > tr:nth-child(1)").css("td[class='data-label']::text").get())
                email = self.none_handler(data_point.css("tbody > tr:nth-child(2)").css("td[class='data-label']::text").get())
                division = self.none_handler(data_point.css("tbody > tr:nth-child(3)").css("td[class='data-label']::text").get())
                unit = self.none_handler(data_point.css("tbody > tr:nth-child(4)").css("td[class='data-label']::text").get())
                
                person_data = {
                    "org_sort":28,
                    "org_id": "MOH",
                    "org_name": "KEMENTERIAN KESIHATAN",
                    "org_type": "ministry",
                    "division_name": division,
                    "division_sort": division_sort_order,
                    "subdivision_name": unit,
                    "position_sort": 10*page_number + person_sort + 1,
                    "person_name": name,
                    "position_name": position,
                    "person_phone": phone,
                    "person_email": email,
                    "person_fax": None,
                    "parent_org_id": None
                }

                yield person_data

            next_page_available = current_page.css("a[class='paginate_button next']")
            
            if next_page_available:
                await page.click("a[class='paginate_button next']")
            else:
                break
        await page.close()