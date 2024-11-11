import re
import scrapy

class KBSSpider(scrapy.Spider):
    name = 'kbs'
    start_urls = [
        "https://www.kbs.gov.my/1-info-kbs/pengurusan-tertinggi.html",
        "https://www.kbs.gov.my/1-info-kbs/ketua-jabatan-agensi.html",
        "https://www.kbs.gov.my/1-info-kbs/ketua-pegawai-digital-cdo.html"
    ]

    def parse(self, response):
        phone_regex = re.compile(r"\d+.+\d")
        email_regex = re.compile(r"[\w\.]+@[\w\.]+")
        unit_regex = re.compile(r"^Pejabat|^Unit")
        html_clean = lambda string: re.sub(pattern=r"<.+?>", repl="", string=string).strip()

        name_lst = [html_clean(block).strip() for block in response.css("span[class^='sppb-person-name']").getall()]
        data_lst = [
            [html_clean(data).strip() for data in re.split(r"<br>|\n", block) if html_clean(data)]
            for block
            in response.css("div[class^='sppb-person-introtext']").getall()
        ]
            
        for person_order, (name, data) in enumerate(zip(name_lst, data_lst)):
            division_name = [
                "Pengurusan Tertinggi",
                div_lst[0] if (div_lst := [string for string in data if string.startswith("Bahagian")]) else None,
                None
            ]
            person_dict = {
                "org_sort": 23,
                "org_id": "KBS",
                "org_name": "KEMENTERIAN BELIA DAN SUKAN",
                "org_type": "ministry",
                "division_sort": self.start_urls.index(response.url)+1,
                "division_name": division_name.pop(self.start_urls.index(response.url)),
                "subdivision_name": div_lst[0] if (div_lst := [string for string in data if re.match(unit_regex, string)]) else None,
                "position_sort": person_order+1,
                "person_name": name,
                "position_name": data[0],
                "person_phone": phone_lst[0] if (phone_lst := [string.replace(" ", "") for string in data if re.match(phone_regex, string)]) else None,
                "person_email": email_lst[0] if (email_lst := [string.replace(" ", "") for string in data if re.match(email_regex, string)]) else None,
                "person_fax": None,
                "parent_org_id": None,
            }
            yield person_dict