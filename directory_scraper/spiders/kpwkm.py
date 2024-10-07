import scrapy

class KPWKMSpider(scrapy.Spider):
    name = "kpwkm"
    allowed_domains = ["kpwkm.gov.my"]
    start_urls = ["https://www.kpwkm.gov.my/kpwkm/index.php?r=portal/left&id=Y3Fkb2hRdnQxeDJRZ3B1WjAvZVhWQT09"]

    bahagian_mapping = [
        {"division_code": "A01", "division_name": "PEJABAT MENTERI"},
        {"division_code": "A02", "division_name": "PEJABAT TIMBALAN MENTERI"},
        {"division_code": "A03", "division_name": "PEJABAT KETUA SETIAUSAHA"},
        {"division_code": "A22", "division_name": "PEJABAT TIMBALAN KETUA SETIAUSAHA (OPERASI)"},
        {"division_code": "A23", "division_name": "PEJABAT TIMBALAN KETUA SETIAUSAHA (STRATEGIK)"},
        {"division_code": "A05", "division_name": "UNIT AUDIT DALAM"},
        {"division_code": "A06", "division_name": "UNIT PENASIHAT UNDANG-UNDANG"},
        {"division_code": "A07", "division_name": "UNIT KOMUNIKASI KORPORAT"},
        {"division_code": "A20", "division_name": "UNIT INTEGRITI"},
        {"division_code": "A09", "division_name": "BAHAGIAN DASAR DAN PERANCANGAN STRATEGIK"},
        {"division_code": "A11", "division_name": "BAHAGIAN HUBUNGAN ANTARABANGSA"},
        {"division_code": "A13", "division_name": "BAHAGIAN KEWANGAN"},
        {"division_code": "A14", "division_name": "BAHAGIAN PEMBANGUNAN"},
        {"division_code": "A15", "division_name": "BAHAGIAN PENGURUSAN SUMBER MANUSIA"},
        {"division_code": "A16", "division_name": "BAHAGIAN KHIDMAT PENGURUSAN"},
        {"division_code": "A17", "division_name": "BAHAGIAN PENGURUSAN MAKLUMAT"},
        {"division_code": "A19", "division_name": "BAHAGIAN AKAUN"},
        {"division_code": "A18", "division_name": "BAHAGIAN KOLABORASI STRATEGIK"}
    ]

    def parse(self, response):
        for division_sort, row in enumerate(self.bahagian_mapping):
            division_code = row["division_code"]
            yield scrapy.Request(
                url=f"https://www.kpwkm.gov.my/kpwkm/index.php?r=portal%2Fleft&id=Y3Fkb2hRdnQxeDJRZ3B1WjAvZVhWQT09&department_code={division_code}&branch_code=0&unit_code=0&section_code=0&DirektoriStaf%5Bname%5D=",
                callback=self.parse_items,
                meta={
                    "division_name": row["division_name"],
                    "division_sort": division_sort+1
                }
            )
    
    def parse_items(self, response):
        division_name = response.meta["division_name"]
        division_sort = response.meta["division_sort"]
        current_unit = None
        person_sort = 1
        for row in response.css("div[class='dataTables_wrapper form-inline no-footer'] > *"):
            if table := row.css("div[id='direktori-staf-grid']").css("tbody > tr"):
                for data_point in table:
                    person_data = {
                        "org_id": "kpwkm",
                        "org_name": "Kementerian Pembangunan Wanita, Keluarga dan Masyarakat",
                        "org_sort": 15,
                        "division_name": division_name,
                        "division_sort": division_sort,
                        "unit_name": current_unit,
                        "person_position": data_point.xpath("*[3]/text()").get(),
                        "person_name": data_point.xpath("*[2]//a/text()").get(),
                        "person_email": data_point.xpath("*[4]/text()").get(),
                        "person_fax": None,
                        "person_phone": data_point.xpath("*[5]/text()").get(),
                        "person_sort": person_sort,
                        "parent_org_id": None,
                    }
                    yield person_data
                    person_sort += 1
            elif unit_name := row.xpath("b/text()").get():
                current_unit = unit_name