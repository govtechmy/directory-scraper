import scrapy

class KbsSpider(scrapy.Spider):
    name = "kbs"
    allowed_domains = ["kbs.gov.my"]
    start_urls = ["https://www.kbs.gov.my/pejabat-menteri/list/1.html"]

    def parse(self, response):
        # List of href values to exclude. These are agencies with different start urls:
        excluded_hrefs = [
            "#",
            "https://www.kbs.gov.my/carian-staf",
            "https://www.jbsn.gov.my/hubungi-kami/direktori-jbsn.html",
            "https://www.nsc.gov.my/direktori-staf/",
            "https://isn.gov.my/direktori/",
            "https://www.iyres.gov.my/hubungi-kami/direktori-pegawai",
            "https://pps.kbs.gov.my/hubungi-kami/direktori.html",
            "https://roy.kbs.gov.my/direktori-staf/direktori-pegawai/list/1.html",
            "https://www.stadium.gov.my/index.php/directory",
            "https://www.adamas.gov.my/index.php/en/contact/direktori-staf"
        ]

        divisions = response.xpath('//select[@name="forma"]/option')
        division_sort = 0
        for division in divisions:
            href = division.xpath('@value').get()
            division_name = division.xpath('text()').get().strip()
            # print(f"Found division: {division_name}, href: {href}")

            if href in excluded_hrefs:
                # print(f"Skipping division: {division_name}, href: {href}")
                continue

            if href and href != "#":
                division_sort += 1  # increment the division_sort counter only for valid divisions
                # print(f"Processing division: {division_name}, href: {href}, division_sort: {division_sort}")
                yield scrapy.Request(
                    url=response.urljoin(href),
                    callback=self.parse_staff,
                    meta={'division_name': division_name, 'division_sort': division_sort}
                )


    def parse_staff(self, response):
        rows = response.xpath('//tr[starts-with(@id, "list_1_com_fabrik_1_row_")]')
        division_name = response.meta.get('division_name')
        division_sort = response.meta.get('division_sort')  
        
        for position_sort_order, row in enumerate(rows, start=1):
            subdivision_name = row.xpath('.//td[contains(@class, "direktori_staff___cawangan_unit")]/text()').get().strip()
            final_subdivision_name = None if subdivision_name == division_name else subdivision_name

            person_email = row.xpath('.//td[contains(@class, "direktori_staff___emel")]/text()').get().strip()
            final_person_email = f"{person_email}@kbs.gov.my" if person_email else None

            yield {
                "org_sort": 23,
                "org_id": "KBS",
                "org_name": "KEMENTERIAN BELIA DAN SUKAN",
                "org_type": "ministry",
                "division_name": division_name,
                "division_sort": division_sort,
                "position_sort_order": position_sort_order,
                "subdivision_name": final_subdivision_name, 
                "person_name": row.xpath('.//td[contains(@class, "direktori_staff___nama")]/text()').get().strip(),
                "position_name": row.xpath('.//td[contains(@class, "direktori_staff___jawatan")]/text()').get().strip(),
                "person_phone": row.xpath('.//td[contains(@class, "direktori_staff___no_telefon")]/text()').get().strip(),
                "person_email": final_person_email,
                "person_fax": None,
                "parent_org_id": None,
            }

        next_page = response.xpath('//a[@rel="next"]/@href').get() 
        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse_staff,
                meta={'division_name': division_name, 'division_sort': division_sort}
            )
