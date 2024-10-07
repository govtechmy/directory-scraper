import re
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class KPNSpider(CrawlSpider):
    name = "kpn"
    allowed_domains = ["www.perpaduan.gov.my"]
    start_urls = [
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-menteri",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-menteri-3",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/pejabat-penasihat-undang-undang-2",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/unit-komunikasi-korporat",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/seksyen-audit-1",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/unit-integriti",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-dasar-dan-hubungan-antarabangsa-1",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-kesepaduan-nasional",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-kolaborasi-strategik",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-pengurusan-maklumat",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-khidmat-pengurusan",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-pengurusan-sumber-manusia",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-kewangan-dan-pembangunan",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-akaun",
    ]
    
    bahagian_mapping = {
        "pejabat-menteri": "Pejabat Menteri",
        "pejabat-timbalan-menteri-3": "Pejabat Timbalan Menteri",
        "pejabat-ketua-setiausaha-2": "Pejabat Ketua Setiausaha",
        "pejabat-penasihat-undang-undang-2": "Pejabat Penasihat Undang Undang",
        "unit-komunikasi-korporat": "Unit Komunikasi Korporat",
        "seksyen-audit-1": "Seksyen Audit",
        "unit-integriti": "Unit Integriti",
        "pejabat-timbalan-ketua-setiausaha": "Pejabat Timbalan Ketua Setiausaha",
        "bahagian-dasar-dan-hubungan-antarabangsa-1": "Bahagian Dasar Dan Hubungan Antarabangsa",
        "bahagian-kesepaduan-nasional": "Bahagian Kesepaduan Nasional",
        "bahagian-kolaborasi-strategik": "Bahagian Kolaborasi Strategik",
        "pejabat-setiausaha-bahagian-kanan-pengurusan": "Pejabat Setiausaha Bahagian Kanan Pengurusan",
        "bahagian-pengurusan-maklumat": "Bahagian Pengurusan Maklumat",
        "bahagian-khidmat-pengurusan": "Bahagian Khidmat Pengurusan",
        "bahagian-pengurusan-sumber-manusia": "Bahagian Pengurusan Sumber Manusia",
        "bahagian-kewangan-dan-pembangunan": "Bahagian Kewangan Dan Pembangunan",
        "bahagian-akaun": "Bahagian Akaun",
    }
    url_suffixes = [f"/bm/.*{url}$" for url in bahagian_mapping.keys()]
    none_handler = lambda self, condition: result.strip() if (result := condition) else "NULL"
    
    rules = (
        Rule(LinkExtractor(
            allow=rf"{'|'.join(url_suffixes)}|\?filter-match=any&start=(\d+)",
            deny=r"/bm/direktori-pegawai-3"), callback='parse_item'
        ),
    )

    def parse_item(self, response):
        unit_name = "NULL"
        person_sort = 1
        
        division_name = [name for url, name in self.bahagian_mapping.items() if url in response.url][-1]
        division_sort = [idx for idx, url in enumerate(self.start_urls) if response.url.startswith(url)]
        css_filter = "div[class='personlist']  > *" + (":nth-child(n+2)" if re.findall(r"\D+$", response.url) else "")
        page_number = int(page_num[0]) if (page_num := re.findall(r"\d+$", response.url)) else 0
        
        for row in response.css(css_filter):
            if not division_sort:
                break
            if row.css("::attr(class)").get() == "heading-group":
                unit_name = row.css("h3 > span::text").get()
            else:
                person_data = {
                    "org_id": "KPN",
                    "org_name": "Kementerian Perpaduan Negara",
                    "org_sort": 22,
                    "org_type": "ministry",
                    "division_name": division_name,
                    "division_sort": division_sort[-1]+1,
                    "unit_name": unit_name,
                    "person_position": row.css("span[aria-label='Position']::text").get(),
                    "person_name": row.css("span[aria-label='Name']::text").get(),
                    "person_email": self.none_handler(row.css("span[aria-label='Email']::text").get()),
                    "person_fax": "NULL",
                    "person_phone": row.css("span[aria-label='Phone']::text").get(),
                    "person_sort": page_number+person_sort
                }
                person_sort += 1
                yield person_data