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

    # Mapping for level2 -> level3 parents
    level2_to_level3_map = {}

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

            self.logger.info(f"Processing division_sort: {valid_sort}, URL: {url}")
            yield scrapy.Request(url=url, callback=self.parse_level2_parents, meta={'division_sort': valid_sort})
            valid_sort += 1

    def parse_level2_parents(self, response):
        division_sort = response.meta.get('division_sort', -1)
        level2_links = sorted(response.css(".level2.parent > a::attr(href)").getall())
        level2_names = sorted(response.css(".level2.parent > a span::text").getall())

        # Create a list to store level3 requests
        level3_requests = []

        for level2_name, level2_link in zip(level2_names, level2_links):
            level2_name = level2_name.strip()
            level2_url = response.urljoin(level2_link)

            # Initialize mapping for level2 -> level3
            if level2_name not in self.level2_to_level3_map:
                self.level2_to_level3_map[level2_name] = []

            self.logger.info(f"Checking Level 2 link: {level2_url}")

            # Add level3_parents request to the list
            level3_requests.append(scrapy.Request(
                url=level2_url,
                callback=self.parse_level3_parents,
                meta={'level2_name': level2_name, 'division_sort': division_sort}
            ))

        # Yield all level3 requests except the last one
        for req in level3_requests[:-1]:
            yield req

        # Yield the last request with parse_items as the final step
        if level3_requests:
            last_request = level3_requests[-1]
            yield scrapy.Request(
                url=last_request.url,
                callback=self.parse_level3_parents,
                meta={
                    'level2_name': last_request.meta['level2_name'],
                    'division_sort': last_request.meta['division_sort'],
                    'response': response,  # Pass original response for parse_items
                    'final_callback': True  # Mark this request as the final callback
                }
            )
        else:
            # If no level2 links, directly trigger parse_items
            yield scrapy.Request(
                url=response.url,
                callback=self.parse_items,
                meta={'division_sort': division_sort},
                dont_filter=True
            )

    def parse_level3_parents(self, response):
        level2_name = response.meta.get('level2_name')
        division_sort = response.meta.get('division_sort')
        is_final_callback = response.meta.get('final_callback', False)
        original_response = response.meta.get('response', None)

        level3_links = response.css(".level3.parent > a span::text").getall()
        level3_names = [link.strip() for link in level3_links]

        if level2_name in self.level2_to_level3_map:
            self.level2_to_level3_map[level2_name].extend(level3_names)

        self.logger.info(f"Mapping Level2: {level2_name} -> Level3: {level3_names}")

        # If this is the final callback, trigger parse_items
        if is_final_callback and original_response:
            self.logger.info(f"Final level3_parents parsed. Triggering parse_items for division_sort {division_sort}.")
            yield scrapy.Request(
                url=original_response.url,
                callback=self.parse_items,
                meta={'division_sort': division_sort},
                dont_filter=True
            )


    def parse_items(self, response):
        division_sort = response.meta.get('division_sort', -1)
        division_name = response.css("h1 ::text").get(default="").strip()
        position_sort = response.meta.get('position_sort', 1)

        level2_parent_links = response.css(".level2.parent > a span::text").getall()
        level2_parent_names = [link.strip() for link in level2_parent_links]

        level2_links = response.css(".level2 > a span::text").getall()
        level2_names = [link.strip() for link in level2_links]

        #=============== SUBDIVISION CLEANUP ===================

        data_cards = response.css("div.uk-overflow-hidden")

        # Sort the data cards based on the text in <h2> (person name) as a key
        sorted_data_cards = sorted(
            data_cards,
            key=lambda card: card.css("h2::text").get(default="").strip()
        )

        # Process sorted data cards
        for position_sort, data_card in enumerate(sorted_data_cards, start=1):
            subdivision_links = data_card.css("a[href^='/index.php']::text").getall()
            subdivision_name = ", ".join([link.strip() for link in subdivision_links])
            subdivision_name_initial = subdivision_name
            
            # Clean up subdivision name by removing division name
            if division_name in subdivision_name:
                subdivision_name = subdivision_name.replace(division_name, "").strip(", ").strip()

            # Determine subdivision_level2_parent
            subdivision_level2_parent = None
            for level2_parent in level2_parent_names:
                if level2_parent in subdivision_name_initial:
                    subdivision_level2_parent = level2_parent
                    subdivision_name = subdivision_name.replace(level2_parent, "").strip(", ").strip()
                    break

            # Determine subdivision_level2
            subdivision_level2 = None
            for level2 in level2_names:
                # Only set subdivision_level2 if it is different from subdivision_level2_parent
                if level2 in subdivision_name_initial and level2 != subdivision_level2_parent:
                    subdivision_level2 = level2
                    subdivision_name = subdivision_name.replace(level2, "").strip(", ").strip()
                    break

            # Determine subdivision_level3_parent
            subdivision_level3_parent = None
            if subdivision_level2_parent in self.level2_to_level3_map:
                for level3_parent in self.level2_to_level3_map[subdivision_level2_parent]:
                    if level3_parent in subdivision_name_initial:
                        subdivision_level3_parent = level3_parent
                        subdivision_name = subdivision_name.replace(level3_parent, "").strip(", ").strip()
                        break
            else:
                # If no level2_parent was resolved, search all level3 mappings
                for level2, level3_list in self.level2_to_level3_map.items():
                    for level3_parent in level3_list:
                        if level3_parent in subdivision_name_initial:
                            subdivision_level3_parent = level3_parent
                            subdivision_name = subdivision_name.replace(level3_parent, "").strip(", ").strip()
                            break
                    if subdivision_level3_parent:  # Exit early if a match is found
                        break

            # To identify the missing subdivision_level3_parent using the level2_to_level3_map mapping list, and by checking the value of "final" subdivision_name. So that we can have a hierarchy ">".
            if not subdivision_level3_parent and subdivision_name:
                for level2, level3_list in self.level2_to_level3_map.items():
                    for level3_parent in level3_list:
                        normalized_level3_parent = re.sub(r"[^\w\s]", "", level3_parent).lower().strip()
                        normalized_subdivision_name = re.sub(r"[^\w\s]", "", subdivision_name).lower().strip()
                        
                        self.logger.debug(f"Normalized Check: level3_parent='{normalized_level3_parent}' vs subdivision_name='{normalized_subdivision_name}'")

                        # Check for substring match
                        if normalized_level3_parent in normalized_subdivision_name:
                            self.logger.debug(f"Fallback match found: '{level3_parent}' in '{subdivision_name}'")
                            subdivision_name = subdivision_name.replace(level3_parent, "").strip(", ").strip()
                            # subdivision_name = f"{level3_parent} > {subdivision_name}"
                            subdivision_level3_parent = level3_parent
                            break
                    if subdivision_level3_parent:  # Exit early if a match is found
                        break

            # cleanup for cases like "Seksyen Audit Prestasi > Seksyen Audit Prestasi 1 / ICT > 1 / ICT" -> "Seksyen Audit Prestasi > Seksyen Audit Prestasi 1 / ICT"
            if subdivision_name and subdivision_level3_parent and subdivision_name in subdivision_level3_parent:
                self.logger.debug(f"Redundant name found in subdivision_name: '{subdivision_name}' matches subdivision_level3_parent: '{subdivision_level3_parent}'")
                subdivision_name = None

            # Final print for debugging - if no match, is fine. Simply bcs there is no level3 mapping found for this subdivision.
            if not subdivision_level3_parent and subdivision_name:
                self.logger.info(f"⚠️ Still no match for '{subdivision_name}'")
                
            subdivision_name_final = " > ".join(filter(None, [subdivision_level2_parent, subdivision_level2, subdivision_level3_parent, subdivision_name]))
            subdivision_name_final = subdivision_name_final if subdivision_name_final.strip() else None

        #=============== END OF CLEANUP ===================

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
                "subdivision_level3_parent": subdivision_level3_parent,
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
        division_sort = response.meta.get('division_sort', -1) 

        for link in pagination_links:
            next_page_url = response.urljoin(link)
            if next_page_url in self.excluded_links:
                self.excluded_links_found.append(next_page_url)
                self.logger.info(f"Excluded pagination link found and skipped: {next_page_url}")
                continue

            if next_page_url not in visited_pages:
                visited_pages.add(next_page_url)
                self.logger.info(f"Following pagination: {next_page_url}")
                yield scrapy.Request(
                    next_page_url,
                    callback=self.parse_items,
                    meta={
                        'division_sort': division_sort,  
                        'position_sort': position_sort,
                        'visited_pages': visited_pages
                    }
                )
