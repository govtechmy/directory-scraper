import scrapy


class RuralLinkAnggotaSpider(scrapy.Spider):
    name = 'rurallink_anggota'
    start_urls = ['https://direktori.rurallink.gov.my/']

    position_sort = 0
    division_sort_map = {}
    current_division_sort = 0

    def parse(self, response):
        division_name = None
        subdivision_level1_name = None
        subdivision_level2_name = None

        # Select all rows, including division and subdivision headers
        rows = response.xpath('//table[@class="table table-bordered"]/tbody/tr')

        for row in rows:
            division_header = row.xpath('./preceding::h2[1]/button[contains(@style, "background-color: #00376e;")]/text()').get()
            if division_header:
                division_name = division_header.strip()

                # Assign unique division_sort
                if division_name not in self.division_sort_map:
                    self.current_division_sort += 1
                    self.division_sort_map[division_name] = self.current_division_sort
                division_sort = self.division_sort_map[division_name]

            # Check for subdivision_level1_name header
            subdivision_level1_header = row.xpath('./td[@colspan="6" and contains(@style, "background-color: #0087ff;")]')
            if subdivision_level1_header:
                subdivision_level1_name = subdivision_level1_header.xpath('text()').get().strip()
                subdivision_level2_name = None
                continue

            subdivision_level2_header = row.xpath('./td[@colspan="6" and contains(@style, "background-color: #36c2ff;")]')
            if subdivision_level2_header:
                subdivision_level2_name = subdivision_level2_header.xpath('text()').get().strip()
                continue

            if subdivision_level2_name:
                if subdivision_level1_name:
                    final_subdivision_name = f"{subdivision_level1_name} > {subdivision_level2_name}"
                else:
                    final_subdivision_name = subdivision_level2_name
            elif subdivision_level1_name:
                final_subdivision_name = subdivision_level1_name
            else:
                final_subdivision_name = None

            staff_data = row.xpath('./th[@class="text-center"]')
            if staff_data:
                person_name = row.xpath('./td[1]/text()').get().strip()
                position_name = row.xpath('./td[2]/text()').get().strip()
                person_phone = row.xpath('./td[3]/text()').get().strip()
                person_email_prefix = row.xpath('./td[4]/text()').get().strip()

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


                self.position_sort += 1

                yield {
                    'org_sort': 999,
                    'org_id': "RURALLINK",
                    'org_name': 'KEMENTERIAN KEMAJUAN DESA DAN WILAYAH',
                    'org_type': 'ministry',
                    'division_name': division_name,
                    'division_sort': division_sort,
                    'subdivision_level1_name': subdivision_level1_name,
                    'subdivision_level2_name': subdivision_level2_name,
                    'subdivision_name': final_subdivision_name,
                    'person_name': person_name,
                    'position_name': position_name,
                    'person_phone': person_phone,
                    'person_email': person_email,
                    'person_fax': None,
                    'position_sort': self.position_sort,
                    "parent_org_id": None
                }
