import os
import re
import scrapy
import base64

class MODSpider(scrapy.Spider):
    name = "mod"
    allowed_domains = ["direktori.mod.gov.my"]

    # List urls to be excluded from scraping
    excluded_links = [
        "https://direktori.mod.gov.my/index.php/mindef/category/kem-kem-atm-seluruh-negara"
    ]
    excluded_links_found = []

    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 1,
        'AUTOTHROTTLE_MAX_DELAY': 10,
        'RETRY_TIMES': 2,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 408],
        'RETRY_WAIT_TIME': 5,
        'DOWNLOAD_TIMEOUT': 40,
        'CONCURRENT_REQUESTS': 16,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 8,
        'LOG_LEVEL': 'INFO',
    }

    def start_requests(self):
        main_url = "https://direktori.mod.gov.my/index.php/mindef/"
        yield scrapy.Request(url=main_url, callback=self.extract_start_urls)

    def extract_start_urls(self, response):
        division_links = response.css("h2.uk-h3 a::attr(href)").getall()
        sorted_links = [response.urljoin(link) for link in division_links]

        valid_sort = 1 
        for url in sorted_links:
            if url in self.excluded_links:
                self.excluded_links_found.append(url)
                self.logger.info(f"Excluded link found and skipped: {url}")
                continue

            print(f"Processing division_sort: {valid_sort}, URL: {url}")
            yield scrapy.Request(url=url, callback=self.parse_item, meta={'division_sort': valid_sort, 'position_sort': 1})
            valid_sort += 1

    def parse_item(self, response):
        division_name = response.css("h1 ::text").get(default="").strip()
        division_sort = response.meta.get('division_sort', -1)
        position_sort = response.meta.get('position_sort', 1)

        # Extract level2 parent and level2 for subdivision
        level2_parent_links = response.css(".level2.parent > a span::text").getall()
        level2_parent_names = [link.strip() for link in level2_parent_links]

        level2_links = response.css(".level2 > a span::text").getall()
        level2_names = [link.strip() for link in level2_links]

        #=============== SUBDIVISION CLEANUP ===================

        for data_card in response.css("div.uk-overflow-hidden"):
            subdivision_links = data_card.css("a[href^='/index.php']::text").getall()
            subdivision_name = ", ".join([link.strip() for link in subdivision_links])
            subdivision_name_initial = subdivision_name

            # Clean up subdivision name by removing division name
            if division_name in subdivision_name:
                subdivision_name = subdivision_name.replace(division_name, "").strip(", ").strip()

            # Determine subdivision_level2_parent and subdivision_level2
            subdivision_level2_parent = None
            for level2_parent in level2_parent_names:
                if level2_parent in subdivision_name:
                    subdivision_level2_parent = level2_parent
                    subdivision_name = subdivision_name.replace(level2_parent, "").strip(", ").strip()
                    break

            subdivision_level2 = None
            for level2 in level2_names:
                if level2 in subdivision_name:
                    subdivision_level2 = level2
                    subdivision_name = subdivision_name.replace(level2, "").strip(", ").strip()
                    break

            subdivision_name_final = " > ".join(filter(None, [subdivision_level2_parent, subdivision_level2, subdivision_name]))
            subdivision_name_final = subdivision_name_final if subdivision_name_final.strip() else None

            contact_info = data_card.css("ul > li::text").getall()
            person_phone = next((txt.strip() for txt in contact_info if "Telefon" in txt), None)
            person_fax = next((txt.strip().replace("Faks ", "") for txt in contact_info if "Faks" in txt), None)

            # Decode email
            joomla_mail = data_card.css("joomla-hidden-mail")
            first_part = joomla_mail.attrib.get("first", "")
            last_part = joomla_mail.attrib.get("last", "")
            if first_part and last_part:
                email_username = base64.b64decode(first_part).decode("utf-8")
                email_domain = base64.b64decode(last_part).decode("utf-8")
                person_email = f"{email_username}@{email_domain}"
            else:
                person_email = None

            data = {
                "org_sort": 13,
                "org_id": "MOD",
                "org_name": "Kementerian Pertahanan",
                "org_type": "ministry",
                "division_sort": division_sort,
                "division_name": division_name,
                "subdivision_name_initial": subdivision_name_initial,
                "subdivision_level2_parent": subdivision_level2_parent,
                "subdivision_level2": subdivision_level2,
                "subdivision_name": subdivision_name_final, 
                "position_sort": position_sort,
                "person_name": data_card.css("h2::text").get(default="").strip(),
                "position_name": data_card.css("div:not([class])::text").get(default="").strip(),
                "person_phone": person_phone,
                "person_email": person_email,
                "person_fax": person_fax,
                "parent_org_id": None
            }
            yield data

            position_sort += 1

        pagination_links = response.css("ul.uk-pagination li a::attr(href)").getall()
        visited_pages = response.meta.get('visited_pages', set())

        for link in pagination_links:
            next_page_url = response.urljoin(link)
            if next_page_url in self.excluded_links:
                self.excluded_links_found.append(next_page_url)
                self.logger.info(f"Excluded pagination link found and skipped: {next_page_url}")
                continue

            if next_page_url not in visited_pages:
                visited_pages.add(next_page_url)
                print(f"Following pagination: {next_page_url}")
                yield scrapy.Request(
                    next_page_url,
                    callback=self.parse_item,
                    meta={
                        'division_sort': division_sort,
                        'position_sort': position_sort,
                        'visited_pages': visited_pages
                    }
                )