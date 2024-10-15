import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod

class MOHSpider(scrapy.Spider):
    name = "moh"
    allowed_domains = ["www.moh.gov.my"]
    start_urls = ["https://www.moh.gov.my/index.php/edirectory/member_list"]
    
    none_handler = lambda self, condition: result if (result := condition) else None
    
    division_mapping = [
        # {"division_name": "Bahagian Akaun", "division_code": "29", "division_sort_order": 39},
        # {"division_name": "Bahagian Amalan Perubatan", "division_code": "2", "division_sort_order": 26},
        # {"division_name": "Bahagian Dasar Dan Hubungan Antarabangsa", "division_code": "30", "division_sort_order": 40},
        # {"division_name": "Bahagian Kawalan Penyakit", "division_code": "3", "division_sort_order": 27},
        # {"division_name": "Bahagian Kawalselia Radiasi Perubatan", "division_code": "86", "division_sort_order": 22},
        # {"division_name": "Bahagian Kejururawatan", "division_code": "13", "division_sort_order": 36},
        # {"division_name": "Bahagian Kewangan", "division_code": "31", "division_sort_order": 41},
        # {"division_name": "Bahagian Khidmat Pengurusan", "division_code": "7", "division_sort_order": 31},
        # {"division_name": "Bahagian Pemakanan", "division_code": "72", "division_sort_order": 16},
        # {"division_name": "Bahagian Pembangunan", "division_code": "81", "division_sort_order": 19},
        # {"division_name": "Bahagian Pembangunan Kesihatan Keluarga", "division_code": "8", "division_sort_order": 32},
        # {"division_name": "Bahagian Pembangunan Kompetensi", "division_code": "32", "division_sort_order": 18},
        # {"division_name": "Bahagian Pendidikan Kesihatan", "division_code": "9", "division_sort_order": 33},
        # {"division_name": "Bahagian Pengurusan Latihan", "division_code": "33", "division_sort_order": 11},
        # {"division_name": "Bahagian Pengurusan Maklumat", "division_code": "12", "division_sort_order": 35},
        # {"division_name": "Bahagian Perancangan", "division_code": "80", "division_sort_order": 10},
        # {"division_name": "Bahagian Perkembangan Kesihatan Awam", "division_code": "85", "division_sort_order": 21},
        # {"division_name": "Bahagian Perkembangan Perubatan", "division_code": "11", "division_sort_order": 25},
        # {"division_name": "Bahagian Perkhidmatan Kejuruteraan", "division_code": "4", "division_sort_order": 28},
        # {"division_name": "Bahagian Perolehan Dan Penswastaan", "division_code": "34", "division_sort_order": 12},
        # {"division_name": "Bahagian Perubatan Tradisional Dan Komplementari", "division_code": "35", "division_sort_order": 13},
        # {"division_name": "Bahagian Sains Kesihatan Bersekutu", "division_code": "74", "division_sort_order": 17},
        # {"division_name": "Bahagian Sumber Manusia", "division_code": "36", "division_sort_order": 14},
        # {"division_name": "Cawangan Audit Dalam", "division_code": "1", "division_sort_order": 34},
        # {"division_name": "Health Performance Unit (HPU)", "division_code": "124", "division_sort_order": 23},
        # {"division_name": "Pejabat Ketua Pengarah Kesihatan", "division_code": "17", "division_sort_order": 6},
        # {"division_name": "Pejabat Ketua Setiausaha", "division_code": "18", "division_sort_order": 3},
        # {"division_name": "Pejabat Menteri Kesihatan", "division_code": "19", "division_sort_order": 1},
        # {"division_name": "Pejabat Penasihat Undang-Undang", "division_code": "20", "division_sort_order": 37},
        {"division_name": "Pejabat Timbalan Ketua Pengarah Kesihatan (Kesihatan Awam)", "division_code": "63", "division_sort_order": 8},
        {"division_name": "Pejabat Timbalan Ketua Pengarah Kesihatan (Penyelidikan & Sokongan Teknikal)", "division_code": "64", "division_sort_order": 9},
        {"division_name": "Pejabat Timbalan Ketua Pengarah Kesihatan (Perubatan)", "division_code": "24", "division_sort_order": 7},
        {"division_name": "Pejabat Timbalan Ketua Setiausaha (Kewangan)", "division_code": "25", "division_sort_order": 5},
        {"division_name": "Pejabat Timbalan Ketua Setiausaha (Pengurusan)", "division_code": "26", "division_sort_order": 4},
        {"division_name": "Pejabat Timbalan Menteri Kesihatan", "division_code": "27", "division_sort_order": 2},
        {"division_name": "Program Keselamatan Dan Kualiti Makanan", "division_code": "5", "division_sort_order": 29},
        {"division_name": "Program Kesihatan Pergigian", "division_code": "6", "division_sort_order": 30},
        {"division_name": "Program Perkhidmatan Farmasi", "division_code": "28", "division_sort_order": 38},
        {"division_name": "Pusat Kecemerlangan Kesihatan Mental Kebangsaan (NCEMH)", "division_code": "125", "division_sort_order": 24},
        {"division_name": "Unit Integriti", "division_code": "83", "division_sort_order": 20},
        {"division_name": "Unit Komunikasi Korporat", "division_code": "69", "division_sort_order": 15}
    ]


    def start_requests(self):
        for row in self.division_mapping:
            code = row["division_code"]
            url = f"{self.start_urls[0]}/{code}/1"
            print(f"Starting scrape from: {url}")
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
                    "page_number": 0
                },
            )
    
    async def parse(self, response):
        page = response.meta["playwright_page"]
        division_sort_order = response.meta["division_sort_order"]
        while True:
            page_number = response.meta["page_number"]
            current_page = Selector(text=await page.content())

            for person_sort, data_point in enumerate(current_page.css("div[class='profile-detail col-8-12']")):
                name = self.none_handler(data_point.css("a::text").get())
                position = self.none_handler(data_point.css("p::text").get())
                phone = self.none_handler(data_point.css("tbody > tr:nth-child(1)").css("td[class='data-label']::text").get())
                email = self.none_handler(data_point.css("tbody > tr:nth-child(2)").css("td[class='data-label']::text").get())
                division = self.none_handler(data_point.css("tbody > tr:nth-child(3)").css("td[class='data-label']::text").get())
                unit = self.none_handler(data_point.css("tbody > tr:nth-child(4)").css("td[class='data-label']::text").get())
                
                person_data = {
                    "org_id": "MOH",
                    "org_name": "KEMENTERIAN KESIHATAN",
                    "org_sort":28,
                    "org_type": "ministry",
                    "division_name": division,
                    "division_sort": division_sort_order,
                    "subdivision_name": unit,
                    "position_name": position,
                    "person_name": name,
                    "person_email": email,
                    "person_fax": None,
                    "person_phone": phone,
                    "position_sort_order": 10*page_number + person_sort + 1,
                    "parent_org_id": None
                }

                yield person_data

            next_page_available = current_page.css("a[class='paginate_button next']")
            
            if next_page_available:
                print(f"\n\nNEXT PAGE AVAILABLE: {response.meta["page_number"]}")
                await page.click("a[class='paginate_button next']")
                
                response.meta["page_number"] += 1
                try:
                    await page.wait_for_selector("a[class^='paginate_button next disabled']")
                except Exception:
                    print("\n\nLAST PAGE")
                    continue
            else:
                break
        print(f"\n\nCLOSING PAGE: {response.url}")
        await page.close()