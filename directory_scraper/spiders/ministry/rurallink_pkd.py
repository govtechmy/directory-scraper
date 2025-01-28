import scrapy
import json

class RuralLinkPKDSpider(scrapy.Spider):
    name = 'rurallink_pkd'
    start_urls = ['https://direktori.rurallink.gov.my/pkd']
    position_sort = 0
    division_sort_map = {}
    current_division_sort = 100000

    def start_requests(self):
        url = "https://direktori.rurallink.gov.my/pkd"
        params = {
            "nama": "",
            "negeri": "",
            "pkd": "",
            "q": "/pkd",
            "draw": 1,
            "columns[0][data]": "DT_RowIndex",
            "columns[0][searchable]": "false",
            "columns[0][orderable]": "false",
            "columns[1][data]": "gambar",
            "columns[1][orderable]": "false",
            "columns[2][data]": "negeri",
            "columns[2][orderable]": "false",
            "columns[3][data]": "nama",
            "columns[3][orderable]": "false",
            "columns[4][data]": "jawatan",
            "columns[4][orderable]": "false",
            "columns[5][data]": "daerah",
            "columns[5][orderable]": "false",
            "columns[6][data]": "no_tel_pkd",
            "columns[6][orderable]": "false",
            "columns[7][data]": "no_tel",
            "columns[7][orderable]": "false",
            "columns[8][data]": "email",
            "columns[8][orderable]": "false",
            "order[0][column]": 0,
            "order[0][dir]": "asc",
            "start": 0,
            "length": 300,
            "search[value]": "",
            "_": "1738032006762",
        }

        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://direktori.rurallink.gov.my/pkd?negeri=&nama=&pkd=",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/131.0.0.0",
        }

        params = {key: str(value) for key, value in params.items()}

        yield scrapy.FormRequest(url=url, method="GET", formdata=params, headers=headers, callback=self.parse)

    def parse(self, response):
        try:
            data = json.loads(response.text)
            for item in data.get("data", []):
                self.position_sort += 1

                division_name = item.get("negeri")
                if division_name not in self.division_sort_map:
                    self.current_division_sort += 1
                    self.division_sort_map[division_name] = self.current_division_sort

                division_sort = self.division_sort_map[division_name]

                if division_name:
                    final_division_name = f"PUSAT KOMUNITI DESA {division_name}"
                else: 
                    final_division_name = "PUSAT KOMUNITI DESA"

                person_email_prefix = item.get("email")
                if person_email_prefix and person_email_prefix != "-":
                    person_email_prefix = (person_email_prefix
                        .replace("[@]", "@")
                        .replace("[at]", "@")
                        .replace("[dot]", ".")
                        .replace("[.]", "."))
                    if "@" in person_email_prefix:
                        person_email = person_email_prefix
                    else:
                        person_email = f"{person_email_prefix}@rurallink.gov.my" 
                else:
                    person_email = None

                yield {
                    'org_sort': 999,
                    'org_id': "RURALLINK",
                    'org_name': 'KEMENTERIAN KEMAJUAN DESA DAN WILAYAH',
                    'org_type': 'ministry',
                    'division_name': final_division_name,
                    'division_sort': division_sort,
                    'subdivision_name': item.get("daerah"),
                    'person_name': item.get("nama"),
                    'position_name': item.get("jawatan"),
                    'person_phone': item.get("no_tel"),
                    'person_email': person_email,
                    'person_fax': None,
                    'position_sort': self.position_sort,
                    "parent_org_id": None,
                }
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {e}")


