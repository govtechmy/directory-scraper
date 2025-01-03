import scrapy
import re

class KPDNNegeriSpider(scrapy.Spider):
    name = "kpdn_negeri"
    start_urls = ["https://insid.kpdn.gov.my/negeri"]

    division_sort_counter = 100000

    clean_text = lambda self, text: re.sub(r"[\u00a0\n\r]+", " ", text).strip() if text else None

    def parse(self, response):
        for state_panel in response.css("div.panel.panel-default"):
            negeri_name = self.clean_text(state_panel.css("div.panel-heading b::text").get())

            for cawangan_link in state_panel.css("div.panel-body a"):
                division_name = self.clean_text(cawangan_link.xpath('normalize-space(.)').get())
                cawangan_url = response.urljoin(cawangan_link.css("::attr(href)").get())

                division_sort = self.division_sort_counter
                self.division_sort_counter += 1

                yield scrapy.Request(
                    url=cawangan_url,
                    callback=self.parse_cawangan,
                    meta={
                        "negeri_name": negeri_name,
                        "division_name": division_name,
                        "division_sort": division_sort,
                    }
                )

    def parse_cawangan(self, response):
        negeri_name = response.meta.get("negeri_name")
        division_name = response.meta.get("division_name")
        division_sort = response.meta.get("division_sort")
        position_counter = 1
        seen_personnel = set()  # track unique staff to avoid duplication

        final_division_name = f"{negeri_name} - {division_name}" if negeri_name else division_name

        for section_panel in response.css("div.panel.panel-default.table-line"):
            subdivision_name = self.clean_text(section_panel.css("div.panel-heading b::text").get())

            # Compare subdivision_name with division_name and set to None if they are equal
            adjusted_subdivision_name = subdivision_name if subdivision_name == division_name else subdivision_name # logic is flawed but keep it

            # Process nested units first
            for unit_panel in section_panel.css("div.row div.panel-heading"):
                unit_name = self.clean_text(unit_panel.css("b::text").get())

                for row in unit_panel.xpath('following-sibling::div[1]//tr'):
                    person_name = self.clean_text(row.css("td a.cm-fat-cursor::text").get())
                    person_email = row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"emel":"(.*?)"')
                    person_phone = row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"no_telefon":"(.*?)"')
                    position_name = self.clean_text(row.css("td:nth-child(2)::text").get())

                    unique_key = person_email or person_name
                    if not person_name or unique_key in seen_personnel:
                        continue  # skip duplicates
                    seen_personnel.add(unique_key)

                    combined_subdivision_name = f"{subdivision_name} > {unit_name}" if unit_name else adjusted_subdivision_name

                    yield {
                        "org_sort": 25,
                        "org_id": "KPDN",
                        "org_name": "KEMENTERIAN PERDAGANGAN DALAM NEGERI DAN KOS SARA HIDUP",
                        "org_type": "ministry",
                        "division_sort": division_sort,
                        "division_name": final_division_name,
                        "subdivision_name": combined_subdivision_name,
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

            # Process top-level personnel only after nested units
            for row in section_panel.css("table.table-custom > tbody > tr"):
                person_name = self.clean_text(row.css("td a.cm-fat-cursor::text").get())
                person_email = row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"emel":"(.*?)"')
                person_phone = row.css("td a.cm-fat-cursor::attr(data-staf)").re_first(r'"no_telefon":"(.*?)"')
                position_name = self.clean_text(row.css("td:nth-child(2)::text").get())

                unique_key = person_email or person_name
                if not person_name or unique_key in seen_personnel:
                    continue  # skip duplicates
                seen_personnel.add(unique_key)

                yield {
                    "org_sort": 25,
                    "org_id": "KPDN",
                    "org_name": "KEMENTERIAN PERDAGANGAN DALAM NEGERI DAN KOS SARA HIDUP",
                    "org_type": "ministry",
                    "division_sort": division_sort,
                    "division_name": final_division_name,
                    "subdivision_name": adjusted_subdivision_name,
                    # "unit_name": None,  # Top-level staff have no unit
                    "position_sort": position_counter,
                    "person_name": person_name,
                    "position_name": position_name,
                    "person_phone": person_phone,
                    "person_email": person_email,
                    "person_fax": None,
                    "parent_org_id": None,
                }
                position_counter += 1
