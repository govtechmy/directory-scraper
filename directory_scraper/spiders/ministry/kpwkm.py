import scrapy

class KPWKMSpider(scrapy.Spider):
    name = "kpwkm"
    allowed_domains = ["kpwkm.gov.my"]
    start_urls = ["https://www.kpwkm.gov.my/kpwkm/index.php?r=portal/left&id=Y3Fkb2hRdnQxeDJRZ3B1WjAvZVhWQT09"]

    bahagian_mapping = []

    def parse(self, response):
        self.bahagian_mapping = [
            {"division_code": row.css("::attr(value)").get(), "division_name": row.css("::text").get()}
            for row in response.css("select[class='form-control'][name='department_code'] option")
            if row.css("::attr(value)").get()
        ]

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
                    email = data_point.xpath("*[4]/text()").get()
                    person_email = f"{email}@kpwkm.gov.my" if (email or email != "-") else None
                    phone = data_point.xpath("*[5]/text()").get()
                    person_phone = phone if (phone and phone != "--") else None
                    person_data = {
                        "org_sort": 15,
                        "org_id": "KPWKM",
                        "org_name": "Kementerian Pembangunan Wanita, Keluarga dan Masyarakat",
                        "org_type": "ministry",
                        "division_sort": division_sort,
                        "division_name": division_name,
                        "subdivision_name": current_unit,
                        "position_sort": person_sort,
                        "person_name": data_point.xpath("*[2]//a/text()").get(),
                        "position_name": data_point.xpath("*[3]/text()").get(),
                        "person_phone": person_phone,
                        "person_email": person_email,
                        "person_fax": None,
                        "parent_org_id": None,
                    }
                    yield person_data
                    person_sort += 1
            elif unit_name := row.xpath("b/text()").get():
                current_unit = unit_name