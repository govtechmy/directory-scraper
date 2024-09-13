import re
import scrapy

class MOFSpider(scrapy.Spider):
    name = 'mof'
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

        directory_lst = list()
        for name, data in zip(name_lst, data_lst):
            person_dict = {
                "agency": phone_lst[0] if (phone_lst := [string for string in data if re.match(r"^Kementerian", string)]) else "Kementerian Belia dan Sukan",
                "person_name": name,
                "division": div_lst[0] if (div_lst := [string for string in data if string.startswith("Bahagian")]) else None,
                "unit": div_lst[0] if (div_lst := [string for string in data if re.match(unit_regex, string)]) else None,
                "person_position": data[0],
                "person_phone": phone_lst[0] if (phone_lst := [string.replace(" ", "") for string in data if re.match(phone_regex, string)]) else None,
                "person_email": email_lst[0] if (email_lst := [string.replace(" ", "") for string in data if re.match(email_regex, string)]) else None
            }
            directory_lst.append(person_dict)

        yield directory_lst