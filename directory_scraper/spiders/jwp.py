import re
import json
import scrapy

class JWPSpider(scrapy.Spider):
    name = "jwp"
    start_urls = [
        "https://www.jwp.gov.my/menteri.html",
        "https://www.jwp.gov.my/directory-list?t=14&ty=top",
        "https://www.jwp.gov.my/directory-list?t=17&ty=top",
        "https://www.jwp.gov.my/directory-list?t=18&ty=top",
        "https://www.jwp.gov.my/directory-list?t=3&ty=top",
        "https://www.jwp.gov.my/directory-list?t=9&ty=top",
        "https://www.jwp.gov.my/directory-list?t=2&ty=top",
        "https://www.jwp.gov.my/directory-list?t=1&ty=top",
        "https://www.jwp.gov.my/directory-list?t=6&ty=top",
        "https://www.jwp.gov.my/directory-list?t=10&ty=top",
        "https://www.jwp.gov.my/directory-list?t=11&ty=top",
        "https://www.jwp.gov.my/directory-list?t=15&ty=top",
        "https://www.jwp.gov.my/directory-list?t=13&ty=top",
        "https://www.jwp.gov.my/directory-list?t=3&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=9&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=2&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=1&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=6&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=10&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=11&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=15&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=13&ty=dept",
        "https://www.jwp.gov.my/directory-list?t=4&ty=dept",
    ]

    def parse(self, response):
        team_names = [team_name.strip() for team_name in response.css("h2 > button::text").getall()]
        division = [name for name in team_names if not re.match(pattern="^(unit|seksyen)", string=name, flags=re.IGNORECASE)]
        unit = [name for name in team_names if re.match(pattern="^(unit|seksyen)", string=name, flags=re.IGNORECASE)]

        current_division = None
        current_unit = None
        directory_data = []
        person_sort_order = 0
        person_data = {
            "agency": "JABATAN WILAYAH PERSEKUTUAN",
            "person_sort_order": person_sort_order,
            "person_name": None,
            "person_position": None,
            "division": current_division,
            "division_sort_order": self.start_urls.index(response.url)+1,
            "unit": current_unit,
            "person_email": None,
            "person_phone": None
        }

        for line in [text.strip() for text in response.css("div[class='accordion-item'] ::text").getall() if text.strip()]:
            if line in division:
                current_division = line
                continue
            elif line in unit:
                current_unit = line
                continue
            else:
                if line.isupper() or line.upper() == "KOSONG":
                    if person_sort_order > 0:
                        directory_data.append(person_data)
                    person_sort_order += 1
                    person_data = {
                        "agency": "JABATAN WILAYAH PERSEKUTUAN",
                        "person_sort_order": person_sort_order,
                        "person_name": None,
                        "person_position": None,
                        "division": current_division,
                        "division_sort_order": None,
                        "unit": current_unit,
                        "person_email": None,
                        "person_phone": None
                    }
                    person_data["person_name"] = line
                elif "[at]" in line:
                    person_data["person_email"] = line.replace("[at]", "@")
                elif line.startswith("03"):
                    person_data["person_phone"] = line
                elif line == "-":
                    continue
                else:
                    person_data["person_position"] = line
            
        for datapoint in directory_data:
            with open("/Users/mydigital/Desktop/KD Work/directory-scraper/data/checkpoints/jwp.json", "a") as f:
                f.write(json.dumps(datapoint, indent=4)+",\n")
            yield datapoint