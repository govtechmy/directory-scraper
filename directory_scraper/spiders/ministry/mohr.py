import re
import scrapy

class MOHRSpider(scrapy.Spider):
    name = "mohr"

    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@mohr.gov.my" if (result := condition) else None

    def start_requests(self):
        main_url = "https://app1.mohr.gov.my/staff/staff_name.php?"
        yield scrapy.Request(
            url=main_url,
            callback=self.extract_divisions
        )

    def extract_divisions(self, response):
        division_options = {
            row.css("::text").get().upper().strip(): row.attrib.get("value", None)
            for row in response.css("select[id='jabatan'] option")
            if row.attrib.get("value")
        }

        yield scrapy.Request(
            url="https://myapp.mohr.gov.my/smp-master/",
            callback=self.extract_hierarchy,
            meta={"division_options": division_options}
        )

    def extract_hierarchy(self, response):
        division_options = response.meta["division_options"]
        hierarchy = dict()
        division_lst = []
        temp_division = None
        for row in response.css("a"):
            row_txt = None
            if clean_txt := [txt.strip() for txt in row.css("*::text").getall() if txt.strip()]:
                row_txt = re.sub(pattern=r"> |(?<=\() | (?=\))", repl="", string=clean_txt[0], flags=re.IGNORECASE).upper()

            if row_txt and "division/" in row.css("::attr(href)").get():
                # Appending division urls from main directory page
                division_lst.append(row_txt.upper())
                
                # Getting divisional parents for units
                if "CAWANGAN" not in row_txt:
                    temp_division = row_txt
                else:
                    hierarchy[row_txt] = temp_division

        # Matching Directory Division name with URL value
        for division_sort, name in enumerate(division_lst):
            max_match = {"name": None, "length": 0}
            for key in division_options.keys():
                key_set = {elem for elem in key.split() if elem}
                name_set = {elem for elem in name.split() if elem}
                match_len = len(key_set.intersection(name_set))
                
                if match_len > max_match["length"]:
                    max_match["name"] = key
                    max_match["length"] = match_len
            
            division_code = division_options.get(max_match['name'])
            if division_code:
                division_url = f"https://app1.mohr.gov.my/staff/staff_name.php?department={division_code}"
            meta_dict = {"division_sort": division_sort, "division_name": name, "hierarchy": hierarchy}
            yield scrapy.Request(
                url=division_url,
                callback=self.parse_item,
                meta=meta_dict
            )

    def parse_item(self, response):
        hierarchy = response.meta["hierarchy"]
        division_sort = response.meta["division_sort"]
        team_name = response.meta["division_name"]
        division_name = hierarchy.get(team_name, team_name)
        unit_name = team_name if division_name != team_name else None

        for position_sort, row in enumerate(response.css("tbody > tr")):
            phone = self.none_handler(row.xpath("td[5]/text()").get())
            yield {
                "org_sort": 29,
                "org_id": "MOHR",
                "org_name": "KEMENTERIAN SUMBER MANUSIA",
                "org_type": "ministry",
                "division_name":division_name,
                "division_sort": division_sort,
                "subdivision_name": unit_name,
                "position_sort": position_sort+1,
                "person_name": self.none_handler(row.xpath("td[1]/text()").get()),
                "position_name": self.none_handler(row.xpath("td[2]/text()").get()),
                "person_phone": phone if (phone and phone != "N/A") else None,
                "person_email": self.email_handler(row.xpath("td[5]/a/text()").get()),
                "person_fax": None,
                "parent_org_id": None
            }