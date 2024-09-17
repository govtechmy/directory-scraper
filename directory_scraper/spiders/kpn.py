import re
import json
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class KPNSpider(CrawlSpider):
    name = "kpn"
    allowed_domains = ["www.perpaduan.gov.my"]
    start_urls = [
        "https://www.perpaduan.gov.my/index.php/bm/direktori-pegawai-3",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-menteri",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-menteri-3",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/pejabat-penasihat-undang-undang-2",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/unit-komunikasi-korporat",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/seksyen-audit-1",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-ketua-setiausaha-2/unit-integriti",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-dasar-dan-hubungan-antarabangsa-1",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-kesepaduan-nasional",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-timbalan-ketua-setiausaha/bahagian-kolaborasi-strategik",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-pengurusan-maklumat",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-khidmat-pengurusan",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-khidmat-pengurusan/seksyen-pengurusan-kualiti-dan-inovasi",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-pengurusan-sumber-manusia",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-kewangan-dan-pembangunan",
        "https://www.perpaduan.gov.my/index.php/bm/pejabat-setiausaha-bahagian-kanan-pengurusan/bahagian-akaun",
    ]
        
    rules = (
        Rule(LinkExtractor(r"/bm/(pejabat|direktori)"), callback='parse_item'),
    )

    def parse_item(self, response):
        print(response.url)
        unit_regex = re.compile(r"Unit|Cawangan|Seksyen")
        division_regex = re.compile(r"Pejabat|")

        def determine_team(string:str) -> str:
            if re.match(string=string, pattern=unit_regex):
                team_type = "unit"
            elif re.match(string=string, pattern=division_regex):
                team_type = "division"
            else:
                team_type = "missing"
            return team_type

        for item in response.css("div[class='personlist'] > div"):
            if item.attrib["class"] == "heading-group":
                current_team = item.css("span::text").get()
                team_type = determine_team(item.css("span::text").get())
                continue
            else:
                person_data = {team_type: current_team}
                for data in item.css("div[class='personinfo'] > div > span"):
                    if data_type := data.attrib.get("title", None):
                        person_data[data_type.lower()] = data.css("span::text").get()
                yield person_data