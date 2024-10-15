import scrapy
from scrapy_playwright.page import PageMethod

class MOESpider(scrapy.Spider):
    name = "moe"
    allowed_domains = ["direktori.moe.gov.my"]
    start_urls = ["https://direktori.moe.gov.my/ajax/public/getdir.php?id=1&textsearch=&selectsearch="]

    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@moe.gov.my" if (result := condition) else None

    division_mapping = [
        {"division_code": "120", "division": "PEJABAT MENTERI PENDIDIKAN", "page_id": 1},
        {"division_code": "121", "division": "PEJABAT TIMBALAN MENTERI PENDIDIKAN", "page_id": 1},
        {"division_code": "122", "division": "PEJABAT KETUA SETIAUSAHA", "page_id": 1},
        {"division_code": "123", "division": "PEJABAT KETUA PENGARAH PENDIDIKAN MALAYSIA", "page_id": 1},
        {"division_code": "205", "division": "PEJABAT TIMBALAN KETUA SETIAUSAHA (PERANCANGAN & PEMBANGUNAN)", "page_id": 1},
        {"division_code": "207", "division": "PEJABAT TIMBALAN KETUA SETIAUSAHA (PENGURUSAN)", "page_id": 1},
        {"division_code": "203", "division": "PEJABAT TIMBALAN KETUA PENGARAH PENDIDIKAN MALAYSIA (SEKTOR DASAR & KURIKULUM)", "page_id": 1},
        {"division_code": "202", "division": "PEJABAT TIMBALAN KETUA PENGARAH PENDIDIKAN MALAYSIA (SEKTOR OPERASI SEKOLAH)", "page_id": 1},
        {"division_code": "204", "division": "PEJABAT TIMBALAN KETUA PENGARAH PENDIDIKAN MALAYSIA (SEKTOR PEMBANGUNAN PROFESIONALISME)", "page_id": 1},
        {"division_code": "404", "division": "BAHAGIAN AUDIT SEKOLAH", "page_id": 1},
        {"division_code": "132", "division": "BAHAGIAN AKAUN", "page_id": 1},
        {"division_code": "181", "division": "BAHAGIAN TAJAAN PENDIDIKAN", "page_id": 1},
        {"division_code": "196", "division": "BAHAGIAN PERANCANGAN STRATEGIK DAN HUBUNGAN ANTARABANGSA", "page_id": 1},
        {"division_code": "182", "division": "BAHAGIAN KEWANGAN", "page_id": 1},
        {"division_code": "185", "division": "BAHAGIAN KHIDMAT PENGURUSAN", "page_id": 1},
        {"division_code": "179", "division": "BAHAGIAN MATRIKULASI", "page_id": 1},
        {"division_code": "183", "division": "BAHAGIAN PEMBANGUNAN", "page_id": 1},
        {"division_code": "189", "division": "BAHAGIAN PEMBANGUNAN KURIKULUM", "page_id": 1},
        {"division_code": "167", "division": "BAHAGIAN PROFESIONALISME GURU", "page_id": 1},
        {"division_code": "168", "division": "BAHAGIAN PENDIDIKAN ISLAM", "page_id": 1},
        {"division_code": "191", "division": "BAHAGIAN PENDIDIKAN KHAS", "page_id": 1},
        {"division_code": "175", "division": "BAHAGIAN SUKAN, KOKURIKULUM DAN KESENIAN", "page_id": 1},
        {"division_code": "171", "division": "BAHAGIAN PENDIDIKAN SWASTA", "page_id": 1},
        {"division_code": "190", "division": "BAHAGIAN PENDIDIKAN DAN LATIHAN TEKNIKAL VOKASIONAL", "page_id": 1},
        {"division_code": "174", "division": "BAHAGIAN PENGURUSAN ASET", "page_id": 1},
        {"division_code": "131", "division": "BAHAGIAN PENGURUSAN MAKLUMAT", "page_id": 1},
        {"division_code": "170", "division": "BAHAGIAN PENGURUSAN SEKOLAH HARIAN", "page_id": 1},
        {"division_code": "176", "division": "BAHAGIAN PENGURUSAN SEKOLAH BERASRAMA PENUH ", "page_id": 1},
        {"division_code": "180", "division": "BAHAGIAN PENGURUSAN SUMBER MANUSIA", "page_id": 1},
        {"division_code": "169", "division": "BAHAGIAN PERANCANGAN DAN PENYELIDIKAN DASAR PENDIDIKAN", "page_id": 1},
        {"division_code": "200", "division": "BAHAGIAN PERMATA", "page_id": 1},
        {"division_code": "173", "division": "BAHAGIAN PEROLEHAN", "page_id": 1},
        {"division_code": "184", "division": "BAHAGIAN PSIKOLOGI DAN KAUNSELING", "page_id": 1},
        {"division_code": "172", "division": "BAHAGIAN SUMBER DAN TEKNOLOGI PENDIDIKAN", "page_id": 1},
        {"division_code": "197", "division": "DEWAN BAHASA DAN PUSTAKA", "page_id": 1},
        {"division_code": "178", "division": "INSTITUT AMINUDDIN BAKI", "page_id": 1},
        {"division_code": "188", "division": "INSTITUT PENDIDIKAN GURU MALAYSIA", "page_id": 1},
        {"division_code": "198", "division": "INSTITUT TERJEMAHAN & BUKU MALAYSIA", "page_id": 1},
        {"division_code": "187", "division": "JEMAAH NAZIR", "page_id": 1},
        {"division_code": "192", "division": "LEMBAGA PEPERIKSAAN", "page_id": 1},
        {"division_code": "199", "division": "MAJLIS PEPERIKSAAN MALAYSIA", "page_id": 1},
        {"division_code": "166", "division": "UNIT AUDIT DALAM", "page_id": 1},
        {"division_code": "186", "division": "UNIT INTEGRITI", "page_id": 1},
        {"division_code": "194", "division": "UNIT KOMUNIKASI KORPORAT", "page_id": 1},
        {"division_code": "133", "division": "PEJABAT PENASIHAT UNDANG-UNDANG", "page_id": 1},
        {"division_code": "403", "division": "ENGLISH LANGUAGE TEACHING CENTRE", "page_id": 1},
        {"division_code": "420", "division": "YAYASAN DIDIK NEGARA", "page_id": 1},
        {"division_code": "421", "division": "PERBADANAN KOTA BUKU", "page_id": 1},
        {"division_code": "108", "division": "JABATAN PENDIDIKAN NEGERI PERLIS", "page_id": 2},
        {"division_code": "105", "division": "JABATAN PENDIDIKAN NEGERI KEDAH", "page_id": 2},
        {"division_code": "114", "division": "JABATAN PENDIDIKAN NEGERI PULAU PINANG", "page_id": 2},
        {"division_code": "107", "division": "JABATAN PENDIDIKAN NEGERI PERAK", "page_id": 2},
        {"division_code": "115", "division": "JABATAN PENDIDIKAN NEGERI SELANGOR", "page_id": 2},
        {"division_code": "111", "division": "JABATAN PENDIDIKAN NEGERI SEMBILAN", "page_id": 2},
        {"division_code": "119", "division": "JABATAN PENDIDIKAN NEGERI MELAKA", "page_id": 2},
        {"division_code": "104", "division": "JABATAN PENDIDIKAN NEGERI JOHOR", "page_id": 2},
        {"division_code": "113", "division": "JABATAN PENDIDIKAN NEGERI PAHANG", "page_id": 2},
        {"division_code": "112", "division": "JABATAN PENDIDIKAN NEGERI TERENGGANU", "page_id": 2},
        {"division_code": "106", "division": "JABATAN PENDIDIKAN NEGERI KELANTAN", "page_id": 2},
        {"division_code": "109", "division": "JABATAN PENDIDIKAN NEGERI SABAH", "page_id": 2},
        {"division_code": "110", "division": "JABATAN PENDIDIKAN NEGERI SARAWAK", "page_id": 2},
        {"division_code": "116", "division": "JABATAN PENDIDIKAN WP KUALA LUMPUR", "page_id": 2},
        {"division_code": "117", "division": "JABATAN PENDIDIKAN WP LABUAN", "page_id": 2},
        {"division_code": "118", "division": "JABATAN PENDIDIKAN WP PUTRAJAYA", "page_id": 2},
        {"division_code": "407", "division": "PPD NEGERI KEDAH", "page_id": 5},
        {"division_code": "408", "division": "PPD PULAU PINANG", "page_id": 5},
        {"division_code": "409", "division": "PPD NEGERI PERAK", "page_id": 5},
        {"division_code": "406", "division": "PPD  NEGERI SELANGOR", "page_id": 5},
        {"division_code": "410", "division": "PPD NEGERI SEMBILAN", "page_id": 5},
        {"division_code": "411", "division": "PPD NEGERI MELAKA", "page_id": 5},
        {"division_code": "412", "division": "PPD NEGERI JOHOR", "page_id": 5},
        {"division_code": "413", "division": "PPD NEGERI PAHANG", "page_id": 5},
        {"division_code": "414", "division": "PPD NEGERI TERENGGANU", "page_id": 5},
        {"division_code": "415", "division": "PPD NEGERI KELANTAN", "page_id": 5},
        {"division_code": "416", "division": "PPD NEGERI SABAH", "page_id": 5},
        {"division_code": "417", "division": "PPD NEGERI SARAWAK", "page_id": 5},
        {"division_code": "418", "division": "PP W.P. KUALA LUMPUR", "page_id": 5},
    ]

    def start_requests(self):
        for division_order, row in enumerate(self.division_mapping):
            url = f"https://direktori.moe.gov.my/ajax/public/getdir.php?id={row['page_id']}&textsearch=&selectsearch={row['division_code']}"
            yield scrapy.Request(
                url=url,
                callback=self.parse_item,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "input[type='checkbox']"),
                    ],
                    "division": row["division"],
                    "division_sort_order": division_order+1
                }
            )

    async def parse_item(self, response):

        page = response.meta["playwright_page"]
        division = response.meta["division"]
        division_sort_order = response.meta["division_sort_order"]
        
        await page.click("input[type='checkbox']")
        person_sort_order = 1
        current_unit = None
        current_subunit = None
        for table in response.css("div[class='panel panel-default']"):
            if unit_name := table.xpath("div[@class='panel-heading']/text()").getall()[-1].strip():
                current_unit = unit_name
            if table.xpath("div[@class='panel-heading']").attrib.get("href"):
                for top_row in table.css(f"div > div > table[class='table table-striped table-bordered table-hover table-responsive']:nth-child(1)").css("tbody > tr"):
                    if not top_row.css("td:nth-child(2)::text"):
                        continue
                    yield {
                        "org_id": "MOE",
                        "org_name": "KEMENTERIAN PENDIDIKAN",
                        "org_sort": 21,
                        "org_type": "ministry",
                        "division_name": division,
                        "division_sort": division_sort_order,
                        "unit_name": current_unit,
                        "person_position": self.none_handler(top_row.css("td:nth-child(3)::text").get()),
                        "person_name": self.none_handler(top_row.css("td:nth-child(2)::text").get()),
                        "person_email": self.email_handler(top_row.css("td:nth-child(4)::text").get()),
                        "person_fax": None,
                        "person_phone": self.none_handler(top_row.css("td:nth-child(5)::text").get()),
                        "person_sort": person_sort_order,
                        "parent_org_id": None
                    }
            else:
                for row in table.css("div[class='row'] > div > div > div[class='panel panel-default']"):
                    if subunit_name := row.css("h4 > a::text").get().strip():
                        current_subunit = subunit_name
                    for data_row in row.css("table > tbody > tr"):
                        yield {
                            "org_id": "MOE",
                            "org_name": "KEMENTERIAN PENDIDIKAN",
                            "org_sort": 21,
                            "org_type": "ministry",
                            "division_name": division,
                            "division_sort": division_sort_order,
                            "subdivision_name": f"{current_unit} > {current_subunit}",
                            "position_name": self.none_handler(data_row.css("td:nth-child(3)::text").get()),
                            "person_name": self.none_handler(data_row.css("td:nth-child(2)::text").get()),
                            "person_email": self.email_handler(data_row.css("td:nth-child(4)::text").get()),
                            "person_fax": None,
                            "person_phone": self.none_handler(data_row.css("td:nth-child(5)::text").get()),
                            "position_sort_order": person_sort_order,
                            "parent_org_id": None
                        }

        await page.close()