import re
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

class KPNSpider(scrapy.Spider):
    name = "kpn"
    allowed_domains = ["www.perpaduan.gov.my"]
    start_urls = ["https://www.perpaduan.gov.my/index.php/bm/direktori-pegawai-3"]

    bahagian_mapping = {}
    seen_divisions = {}

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        """ Extract valid href links only from `#dj-megamenu238`, excluding `.dj-subtree` """
        extracted_urls = set()

        menu_section = response.css("#dj-megamenu238")  
        if not menu_section:
            self.logger.debug("\n`#dj-megamenu238` NOT FOUND! Exiting extraction.")
            return

        for menu in menu_section.css(".dj-up_a"):
            href = menu.css("::attr(href)").get()
            text = menu.css("span::text").get()
            if href and text:
                full_url = response.urljoin(href.strip())  
                extracted_urls.add(full_url)
                self.bahagian_mapping[full_url] = text.strip()

        for submenu in menu_section.css(".dj-subcol a"):
            if submenu.xpath("ancestor::ul[contains(@class, 'dj-subtree')]"):
                continue  # exclude `.dj-subtree`

            href = submenu.css("::attr(href)").get()
            text = submenu.css("::text").get()
            if href and text:
                full_url = response.urljoin(href.strip())
                extracted_urls.add(full_url)
                self.bahagian_mapping[full_url] = text.strip()

        extracted_urls = {url for url in extracted_urls if "/pejabat-" in url}

        # Assign `division_sort` based on `bahagian_mapping` order
        for idx, url in enumerate(self.bahagian_mapping.keys(), start=1):
            division_name = self.bahagian_mapping[url]
            if division_name not in self.seen_divisions:
                self.seen_divisions[division_name] = idx

        self.logger.debug("\nExtracted Start URLs:", extracted_urls)
        self.logger.debug("\nBahagian Mapping:", self.bahagian_mapping)
        self.logger.debug("\nSeen Divisions:", self.seen_divisions)

        for url in extracted_urls:
            yield scrapy.Request(url=url, callback=self.parse_item)

    def parse_item(self, response):

        self.logger.debug(f"\nProcessing Page: {response.url}")

        # Extract `start=X` from URL to determine pagination
        current_start = int(response.url.split("start=")[-1]) if "start=" in response.url else 0
        items_per_page = 20  # Assumption: Each page has 20 items
        base_person_sort = current_start + 1

        if "division_name" in response.meta:
            division_name = response.meta["division_name"]
        else:
            division_name = self.bahagian_mapping.get(response.url, "Unknown Section")

        # Fallback: Extract division name from the page itself (h3.heading)
        if division_name == "Unknown Section":
            division_name = response.css(".personlist .heading-group h3.heading span::text").get() or "Unknown Section"

        division_sort = self.seen_divisions.get(division_name, 999999)

        unit_names = response.css(".personlist .heading-group h3.heading span::text").getall()
        current_unit_index = 0
        persons = response.css(".personlist .person")

        person_sort = base_person_sort

        for person in persons:
            heading_before = person.xpath("preceding-sibling::div[contains(@class, 'heading-group')][1]/h3/span/text()").get()
            if heading_before and heading_before in unit_names:
                current_unit_index = unit_names.index(heading_before)

            unit_name = unit_names[current_unit_index] if unit_names else None
            final_unit_name = None if unit_name == division_name else unit_name

            person_name = person.css("span[aria-label='Name']::text").get()
            position_name = person.css("span[aria-label='Position']::text").get()
            person_phone = person.css("span[aria-label='Phone']::text").get()
            person_email = person.css("span[aria-label='Email']::text").get()

            if person_email:
                if person_email.startswith('-'):
                    person_email = None
                else:
                    person_email = person_email
            else:
                person_email = None

            person_data = {
                "org_sort": 22,
                "org_id": "KPN",
                "org_name": "Kementerian Perpaduan Negara",
                "org_type": "ministry",
                "division_sort": division_sort,
                "division_name": division_name,
                "subdivision_name": final_unit_name,
                "position_sort_order": person_sort,
                "person_name": person_name,
                "position_name": position_name,
                "person_phone": person_phone,
                "person_email": person_email,
                "person_fax": None,
                "parent_org_id": None,
            }
            person_sort += 1
            yield person_data

        pagination_links = response.css("ul.pagination li a::attr(href)").getall()
        pagination_titles = response.css("ul.pagination li a::attr(title)").getall()

        for title, link in zip(pagination_titles, pagination_links):
            self.logger.debug(f"Title: {title} | URL: {response.urljoin(link)}")

        # Extract highest "start=" value from numbered pages
        next_pages = []
        for title, link in zip(pagination_titles, pagination_links):
            if title.isdigit():  # Check if title is a number
                next_pages.append((int(title), link))  # Store as (page_number, link)

        if next_pages:
            next_pages.sort()
            current_page = int(response.css(".counterpagination .counter::text").re_first(r"Halaman (\d+) dari (\d+)", default="1"))
            total_pages = int(response.css(".counterpagination .counter::text").re_first(r"Halaman \d+ dari (\d+)", default="1"))

            self.logger.debug(f"\nPagination Counter - Current: {current_page}, Total: {total_pages}")

            if current_page < total_pages:
                for page_num, link in next_pages:
                    if page_num > current_page:
                        next_page_url = response.urljoin(link)
                        self.logger.debug(f"\nFollowing Next Page: {next_page_url}")
                        yield scrapy.Request(
                            url=next_page_url,
                            callback=self.parse_item,
                            meta={"division_name": division_name}
                        )
                        break  # stop after getting the next valid page
