import re
import scrapy
from scrapy_playwright.page import PageMethod

class MODSpider(scrapy.Spider):
    name = "mod"
    allowed_domains = ["direktori.mod.gov.my"]
    
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'RETRY_TIMES': 5,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408],
        'RETRY_WAIT_TIME': 10,
        'DOWNLOAD_TIMEOUT': 40,
        'CONCURRENT_REQUESTS': 8,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 2,
        'LOG_LEVEL': 'ERROR',  # Use 'DEBUG' for more detailed logs
    }
    
    start_urls = [
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-menteri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-menteri-pertahanan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-menteri-pertahanan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-ketua-setiausaha",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-ketua-setiausaha/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-dasar",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan-2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan-3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan-4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-komunikasi-strategik",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-komunikasi-strategik/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-komunikasi-strategik/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/jhev",
        "https://direktori.mod.gov.my/index.php/mindef/category/jhev/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/jhev/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/depot-simpanan-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/depot-simpanan-pertahanan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/depot-simpanan-pertahanan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/depot-simpanan-pertahanan/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/urusetia-majlis-angkatan-tentera-malaysia",
        "https://direktori.mod.gov.my/index.php/mindef/category/urusetia-majlis-angkatan-tentera-malaysia/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-pembangunan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan/12",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/12",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/12",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/13",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/14",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/15",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan/16",
        "https://direktori.mod.gov.my/index.php/mindef/category/jabatan-ketua-hakim-peguam",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-integriti",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-integriti/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/stride",
        "https://direktori.mod.gov.my/index.php/mindef/category/stride/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/stride/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/mindef-sabah",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-menteri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-menteri-pertahanan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-pengurusan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat/12",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/5",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/6",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/7",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/8",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/9",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/10",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/11",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/12",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/13",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/14",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/15",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/16",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/17",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/18",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia/19",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-undang-undang",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-undang-undang/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-industri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-industri-pertahanan/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/mafca",
        "https://direktori.mod.gov.my/index.php/mindef/category/mafca/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/midas",
        "https://direktori.mod.gov.my/index.php/mindef/category/midas/2",
        "https://direktori.mod.gov.my/index.php/mindef/category/midas/3",
        "https://direktori.mod.gov.my/index.php/mindef/category/midas/4",
        "https://direktori.mod.gov.my/index.php/mindef/category/mindef-sarawak"
    ]
    
    directory_lst = [
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-menteri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-ketua-setiausaha",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-dasar",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-kewangan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-dasar-perancangan-strategik",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-khidmat-pengurusan",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-komunikasi-strategik",
        "https://direktori.mod.gov.my/index.php/mindef/category/jhev",
        "https://direktori.mod.gov.my/index.php/mindef/category/depot-simpanan-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/urusetia-majlis-angkatan-tentera-malaysia",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-audit-dalam-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-pembangunan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pembangunan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-akaun/",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-perolehan",
        "https://direktori.mod.gov.my/index.php/mindef/category/jabatan-ketua-hakim-peguam",
        "https://direktori.mod.gov.my/index.php/mindef/category/unit-integriti",
        "https://direktori.mod.gov.my/index.php/mindef/category/stride",
        "https://direktori.mod.gov.my/index.php/mindef/category/mindef-sabah",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-menteri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/pejabat-timbalan-ketua-setiausaha-pengurusan",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-maklumat",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-pengurusan-sumber-manusia",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-undang-undang",
        "https://direktori.mod.gov.my/index.php/mindef/category/bahagian-industri-pertahanan",
        "https://direktori.mod.gov.my/index.php/mindef/category/mafca",
        "https://direktori.mod.gov.my/index.php/mindef/category/midas",
        "https://direktori.mod.gov.my/index.php/mindef/category/mindef-sarawak"
    ]
    
    def start_requests(self):
        for url in self.start_urls:
            try_count = 1
            while try_count < 3:
                try:
                    print(f"Processing URL: {url} (try {try_count})")
                    yield scrapy.Request(
                        url=url,
                        callback=self.parse_item,
                        meta={
                            "playwright": True,
                            "playwright_include_page": True,
                            "playwright_page_methods": [
                                PageMethod("wait_for_selector", "div[class='uk-overflow-hidden']"),
                            ],
                        }
                    )
                    break
                except Exception:
                    try_count += 1
                    pass

    async def parse_item(self, response):
        page = response.meta["playwright_page"]

        current_division = response.css("h1 ::text").get()
        division_sort = min([idx for idx, base_url in enumerate(self.directory_lst) if base_url in response.url])+1

        for sort_order, data_card in enumerate(response.css("div[class='uk-overflow-hidden']")):
            page_number = int(page_number[0]) if (page_number := re.findall(r"\d+$", response.url)) else 1
            unit_lst = [elem for elem in data_card.css("a[href^='/index.php']::text").getall() if elem != current_division]
            data = {
                "org_id": "MOD",
                "org_name": "Kementerian Pertahanan",
                "org_sort": 13,
                "org_type": "ministry",
                "division_name": current_division,
                "division_sort": division_sort,
                "subdivision_name": unit_name if (unit_name := " > ".join(unit_lst)) else "",
                "position_name": position.strip() if (position := data_card.css("div:not([class])::text").get()) else None,
                "person_name": name.strip() if (name := data_card.css("h2::text").get()) else None,
                "person_email": data_card.css("joomla-hidden-mail::text").get(),
                "person_fax": fax[0] if (fax := [txt.strip() for txt in data_card.css("ul > li::text").getall() if "Faks" in txt]) else None,
                "person_phone": phone[0] if (phone := [txt.strip() for txt in data_card.css("ul > li::text").getall() if "Telefon" in txt]) else None,
                "person_sort": 10*(page_number-1) + sort_order,
                "parent_org_id": None
            }
            yield data

        await page.close()