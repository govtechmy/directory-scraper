import scrapy
import re

class KPDNSpider(scrapy.Spider):
    name = "kpdn"
    start_urls = ["https://insid.kpdn.gov.my/hq"]

    none_handler = lambda self, condition: result.strip() if (result := condition) else None
    email_handler = lambda self, condition: f"{result}@kpdn.gov.my" if (result := re.sub(r"/txt2img/", "", condition)) else None
    clean_text = lambda self, text: re.sub(r"[\u00a0\n\r]+", " ", text).strip() if text else None

    def parse(self, response):
        """
        Parse the main page to extract division URLs and names.
        """
        for idx, link in enumerate(response.css('div.panel-body a'), start=1):
            href = link.css('::attr(href)').get()
            raw_division_name = link.xpath('normalize-space(.)').get()
            division_name = self.clean_text(raw_division_name)
            full_url = response.urljoin(href)

            yield scrapy.Request(
                url=full_url,
                callback=self.parse_division,
                meta={'division_name': division_name, 'division_sort': idx}
            )
                
    def parse_division(self, response):
        """
        Parse the division page to extract person data, prioritizing nested sections than top-level.
        """
        division_name = response.meta.get('division_name')
        division_sort = response.meta.get('division_sort')

        position_counter = 1  
        seen_personnel = set()  # Track unique personnel by email or name for filtering. To differentiate between top-level and nested-level

        for subdivision_panel in response.css('div.panel.panel-default.table-line'):
            subdivision_name = self.clean_text(subdivision_panel.css('div.panel-heading b::text').get())

            for subunit_panel in subdivision_panel.css('div.row div.panel-heading'):
                unit_name = self.clean_text(subunit_panel.css('b::text').get())

                # Combine subdivision_name and unit_name
                combined_subdivision_name = f"{subdivision_name} > {unit_name}" if unit_name else subdivision_name
                final_combined_subdivision_name = None if combined_subdivision_name == division_name else combined_subdivision_name


                for row in subunit_panel.xpath('following-sibling::div[1]//tr'):
                    person_name = self.clean_text(row.css("td a.cm-fat-cursor::text").get())
                    person_email = self.none_handler(row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"emel":"(.*?)"'))

                    if not person_name or (person_email and person_email in seen_personnel):
                        continue  # Skip duplicates based on email
                    if person_email:
                        seen_personnel.add(person_email)  # Track email

                    person_phone = self.none_handler(row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"no_telefon":"(.*?)"'))
                    position_name = self.clean_text(row.css('td:nth-child(2)::text').get())

                    yield {
                        "org_sort": 25,
                        "org_id": "KPDN",
                        "org_name": "KEMENTERIAN PERDAGANGAN DALAM NEGERI DAN KOS SARA HIDUP",
                        "org_type": "ministry",
                        "division_sort": division_sort,
                        "division_name": division_name,
                        "subdivision_name": final_combined_subdivision_name,
                        # "unit_name": unit_name,
                        "position_sort": position_counter,
                        "person_name": person_name,
                        "position_name": position_name,
                        "person_phone": person_phone,
                        "person_email": person_email,
                        "person_fax": None,
                        "parent_org_id": None,
                    }
                    position_counter += 1

        # Parse top-level personnel only if they are not in nested units
        for panel in response.css("div.panel.panel-default.table-line"):
            top_level_unit_name = self.clean_text(panel.css("div.panel-heading b::text").get())  # Dynamic unit name per panel

            for row in panel.css("table.table-custom > tbody > tr"):
                person_name = self.clean_text(row.css("td a.cm-fat-cursor::text").get())
                person_email = self.none_handler(row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"emel":"(.*?)"'))

                if not person_name or (person_email and person_email in seen_personnel):
                    continue  # Skip duplicates based on email
                if person_email:
                    seen_personnel.add(person_email)  # Track email

                person_phone = self.none_handler(row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"no_telefon":"(.*?)"'))
                position_name = self.clean_text(row.css("td:nth-child(2)::text").get())

                top_level_subdivision_name = top_level_unit_name if top_level_unit_name else division_name
                final_subdivision_name = None if top_level_subdivision_name == division_name else top_level_subdivision_name

                yield {
                    "org_sort": 25,
                    "org_id": "KPDN",
                    "org_name": "KEMENTERIAN PERDAGANGAN DALAM NEGERI DAN KOS SARA HIDUP",
                    "org_type": "ministry",
                    "division_sort": division_sort,
                    "division_name": division_name,
                    "subdivision_name": final_subdivision_name,
                    "position_sort": position_counter,
                    "person_name": person_name,
                    "position_name": position_name,
                    "person_phone": person_phone,
                    "person_email": person_email,
                    "person_fax": None,
                    "parent_org_id": None,
                }
                position_counter += 1
