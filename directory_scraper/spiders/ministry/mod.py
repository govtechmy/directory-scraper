import os
import re
import scrapy
import base64
from html import unescape

class MODSpider(scrapy.Spider):
    name = "mod"
    allowed_domains = ["direktori.mod.gov.my"]

    excluded_links = [
        "https://direktori.mod.gov.my/index.php/mindef/category/kem-kem-atm-seluruh-negara"
    ]
    excluded_links_found = []

    # Initialize mappings
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.level1_sort_map = {}
        self.level2_sort_map = {}
        self.level3_sort_map = {}
        self.level4_sort_map = {}
        self.level1_to_level2_map = {}
        self.level2_to_level3_map = {}
        self.level3_to_level4_map = {}
        self.collected_items = []
        self.global_position_sort = 1
        self.global_collected_items = []
        self.global_person_sort = 1

    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1,
        "AUTOTHROTTLE_MAX_DELAY": 10,
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 408],
        "RETRY_WAIT_TIME": 5,
        "DOWNLOAD_TIMEOUT": 40,
        "CONCURRENT_REQUESTS": 16,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "LOG_LEVEL": "INFO",
    }

    def start_requests(self):
        main_url = "https://direktori.mod.gov.my/index.php"
        yield scrapy.Request(url=main_url, callback=self.extract_hierarchy)

    def calculate_indent_level(self, text):
        """Calculate the indentation level based on `. ` patterns (to determine the level sorts)"""
        pattern = re.findall(r"\.\s+|\.\&nbsp;", text)
        return len(pattern)

    def clean_text(self, text):
        """Clean the text to remove formatting characters and artifacts."""
        text = unescape(text)  # Decode HTML entities
        text = re.sub(r"\.\xa0\xa0\xa0", "", text)  # Remove dots and non-breaking spaces
        text = re.sub(r"^\s*-+\s*", "", text)  # Remove only leading dashes and spaces
        return text.strip()

    def build_hierarchy(self, raw_entries):
        """Construct the hierarchy from extracted raw entries."""
        hierarchy = {}
        current_parents = {0: hierarchy}  # Tracks the most recent parent at each level

        for entry in raw_entries:
            level = entry["level"]
            text = entry["text"]

            if level == 0:
                if text not in hierarchy:
                    hierarchy[text] = {}
                current_parents[0] = hierarchy[text]
            else:
                parent = current_parents.get(level - 1)
                if parent is not None:
                    if text not in parent:
                        parent[text] = {}
                    current_parents[level] = parent[text]
                else:
                    self.logger.warning(f"Orphan entry '{text}' at level {level}")

        return hierarchy

    def populate_mappings(self, hierarchy):
        """
        Generate and populate sort mappings and hierarchy relationships with unique keys.
        """
        level1_sort = 1
        level2_sort = 1
        level3_sort = 1
        level4_sort = 1

        # Traverse the hierarchy and assign sort numbers
        for level1_name, level1_children in hierarchy.items():
            # Create a unique key for LEVEL1
            level1_key = level1_name

            # Assign a unique sort number for LEVEL1
            if level1_key not in self.level1_sort_map:
                self.level1_sort_map[level1_key] = level1_sort
                level1_sort += 1

            # Initialize LEVEL1 to LEVEL2 mapping
            self.level1_to_level2_map[level1_key] = []
            for level2_name, level2_children in level1_children.items():
                # Create a unique key for LEVEL2
                level2_key = f"{level1_name}::{level2_name}"

                if level2_key not in self.level2_sort_map:
                    self.level2_sort_map[level2_key] = level2_sort
                    level2_sort += 1
                # Map LEVEL2 to its LEVEL1 parent
                self.level1_to_level2_map[level1_key].append(level2_key)

                # Initialize LEVEL2 to LEVEL3 mapping
                self.level2_to_level3_map[level2_key] = []
                for level3_name, level3_children in level2_children.items():
                    # Create a unique key for LEVEL3
                    level3_key = f"{level2_key}::{level3_name}"

                    if level3_key not in self.level3_sort_map:
                        self.level3_sort_map[level3_key] = level3_sort
                        level3_sort += 1
                    # Map LEVEL3 to its LEVEL2 parent
                    self.level2_to_level3_map[level2_key].append(level3_key)

                    # Initialize LEVEL3 to LEVEL4 mapping
                    self.level3_to_level4_map[level3_key] = []
                    for level4_name, level4_children in level3_children.items():
                        # Create a unique key for LEVEL4
                        level4_key = f"{level3_key}::{level4_name}"

                        if level4_key not in self.level4_sort_map:
                            self.level4_sort_map[level4_key] = level4_sort
                            level4_sort += 1
                        # Map LEVEL4 to its LEVEL3 parent
                        self.level3_to_level4_map[level3_key].append(level4_key)

        self.logger.debug(f"\nLEVEL1 Sort Map: {self.level1_sort_map}")
        self.logger.debug(f"\nLEVEL2 Sort Map: {self.level2_sort_map}")
        self.logger.debug(f"\nLEVEL3 Sort Map: {self.level3_sort_map}")
        self.logger.debug(f"\nLEVEL4 Sort Map: {self.level4_sort_map}")
        self.logger.debug(f"\nLEVEL1 to LEVEL2 Map: {self.level1_to_level2_map}")
        self.logger.debug(f"\nLEVEL2 to LEVEL3 Map: {self.level2_to_level3_map}")
        self.logger.debug(f"\nLEVEL3 to LEVEL4 Map: {self.level3_to_level4_map}")

    def extract_hierarchy(self, response):
        """Step 1: Parse the hierarchy from the main page."""
        print("Starting to extract hierarchy...")
        options = response.css("select#elements_itemcategory-0 option")
        raw_entries = []

        for option in options:
            raw_text = option.css("::text").get()
            if not raw_text:
                self.logger.warning("Skipping option with no text.")
                continue

            raw_text = unescape(raw_text.strip())
            indent_level = self.calculate_indent_level(raw_text)
            cleaned_text = self.clean_text(raw_text)

            raw_entries.append({"level": indent_level, "text": cleaned_text})

        self.hierarchy = self.build_hierarchy(raw_entries)
        self.populate_mappings(self.hierarchy)

        print(f"\nHIERARCHY EXTRACTED: \n{self.hierarchy}")
        self.logger.debug(f"Excluded links found: {self.excluded_links_found}")
        print(f"\nLEVEL1 to LEVEL2: \n{self.level1_to_level2_map}")
        print(f"\nLEVEL2 to LEVEL3: \n{self.level2_to_level3_map}")
        print(f"\nLEVEL3 to LEVEL4: \n{self.level3_to_level4_map}")

        # Step 2: Go to the second URL for division links
        division_url = "https://direktori.mod.gov.my/index.php/mindef/"
        yield scrapy.Request(
            url=division_url,
            callback=self.parse_divisions,
            meta={"hierarchy": self.hierarchy},
        )

    def populate_mappings(self, hierarchy):
        """
        Generate and populate sort mappings and hierarchy relationships with unique keys.
        """
        level1_sort = 1
        level2_sort = 1
        level3_sort = 1
        level4_sort = 1

        # Traverse the hierarchy and assign sort numbers
        for level1_name, level1_children in hierarchy.items():
            # Create a unique key for LEVEL1
            level1_key = level1_name

            # Assign a unique sort number for LEVEL1
            if level1_key not in self.level1_sort_map:
                self.level1_sort_map[level1_key] = level1_sort
                level1_sort += 1

            # Initialize LEVEL1 to LEVEL2 mapping
            self.level1_to_level2_map[level1_key] = []
            for level2_name, level2_children in level1_children.items():
                # Create a unique key for LEVEL2
                level2_key = f"{level1_name}::{level2_name}"

                if level2_key not in self.level2_sort_map:
                    self.level2_sort_map[level2_key] = level2_sort
                    level2_sort += 1
                # Map LEVEL2 to its LEVEL1 parent
                self.level1_to_level2_map[level1_key].append(level2_key)

                # Initialize LEVEL2 to LEVEL3 mapping
                self.level2_to_level3_map[level2_key] = []
                for level3_name, level3_children in level2_children.items():
                    # Create a unique key for LEVEL3
                    level3_key = f"{level2_key}::{level3_name}"

                    if level3_key not in self.level3_sort_map:
                        self.level3_sort_map[level3_key] = level3_sort
                        level3_sort += 1
                    # Map LEVEL3 to its LEVEL2 parent
                    self.level2_to_level3_map[level2_key].append(level3_key)

                    # Initialize LEVEL3 to LEVEL4 mapping
                    self.level3_to_level4_map[level3_key] = []
                    for level4_name, level4_children in level3_children.items():
                        # Create a unique key for LEVEL4
                        level4_key = f"{level3_key}::{level4_name}"

                        if level4_key not in self.level4_sort_map:
                            self.level4_sort_map[level4_key] = level4_sort
                            level4_sort += 1
                        # Map LEVEL4 to its LEVEL3 parent
                        self.level3_to_level4_map[level3_key].append(level4_key)

        self.logger.debug(f"\nLEVEL1 Sort Map: {self.level1_sort_map}")
        self.logger.debug(f"\nLEVEL2 Sort Map: {self.level2_sort_map}")
        self.logger.debug(f"\nLEVEL3 Sort Map: {self.level3_sort_map}")
        self.logger.debug(f"\nLEVEL4 Sort Map: {self.level4_sort_map}")
        self.logger.debug(f"\nLEVEL1 to LEVEL2 Map: {self.level1_to_level2_map}")
        self.logger.debug(f"\nLEVEL2 to LEVEL3 Map: {self.level2_to_level3_map}")
        self.logger.debug(f"\nLEVEL3 to LEVEL4 Map: {self.level3_to_level4_map}")

    def parse_divisions(self, response):
        """
        Extract division links from the second URL.
        """
        print("Extracting division links...")
        divisions = response.css("ul.zoo-category-warp6 li a")
        hierarchy = response.meta.get("hierarchy", {})

        for division in divisions:
            division_url = response.urljoin(division.css("::attr(href)").get())
            division_name = division.css("span::text").get().strip()

            if division_url in self.excluded_links:
                self.logger.debug(f"Skipping excluded division: {division_name} ({division_url})")
                continue

            if not division_name:
                self.logger.warning(f"Missing division name for URL: {division_url}")

            print(f"Scraping division: {division_name} - ({division_url})")
            yield scrapy.Request(
                url=division_url,
                callback=self.extract_person_details,
                meta={"division_name": division_name, "hierarchy": hierarchy},
            )

    def extract_person_details(self, response):
        """
        Extract person details for a specific division and include all required keys in the output.
        """
        division_name = response.meta.get("division_name", "Unknown Division")
        if division_name == "Unknown Division":
            self.logger.error("Division name is missing from meta. Check the previous steps.")
            return  # Exit to avoid processing incomplete data

        hierarchy = response.meta.get("hierarchy", {})
        division_sort = self.level1_sort_map.get(division_name, 0)

        data_cards = response.css("div.uk-overflow-hidden")
        if not data_cards:
            self.logger.warning(f"No data cards found for division: {division_name}")
            return

        # Process each data card
        for data_card in data_cards:
            person_name = data_card.css("h2::text").get(default="").strip()
            position_name = data_card.css("div:not([class])::text").get(default="").strip()
            contact_info = data_card.css("ul > li::text").getall()
            person_phone = next((txt.strip() for txt in contact_info if "Telefon" in txt), None)
            person_fax = next((txt.strip().replace("Faks ", "") for txt in contact_info if "Faks" in txt), None)

            # Decode email
            joomla_mail = data_card.css("joomla-hidden-mail")
            first_part = joomla_mail.attrib.get("first", "")
            last_part = joomla_mail.attrib.get("last", "")
            person_email = None
            if first_part and last_part:
                email_username = base64.b64decode(first_part).decode("utf-8")
                email_domain = base64.b64decode(last_part).decode("utf-8")
                person_email = f"{email_username}@{email_domain}"

            subdivision_links = data_card.css("a[href^='/index.php']::text").getall()
            subdivision_name_initial = ", ".join([link.strip() for link in subdivision_links])

            cleaned_data = self.clean_subdivision_name(
                division_name,
                subdivision_name_initial
            )

            level2_key = f"{division_name}::{cleaned_data.get('subdivision_level2', '')}"
            level3_key = f"{level2_key}::{cleaned_data.get('subdivision_level3', '')}"
            level4_key = f"{level3_key}::{cleaned_data.get('subdivision_level4', '')}"
            
            level2_sort = self.level2_sort_map.get(level2_key, 0)
            level3_sort = self.level3_sort_map.get(level3_key, 0)
            level4_sort = self.level4_sort_map.get(level4_key, 0)
            
            data = {
                "org_sort": 13,
                "org_id": "MOD",
                "org_name": "Kementerian Pertahanan",
                "org_type": "ministry",
                "division_sort": division_sort,
                "level2_sort": level2_sort,
                "level3_sort": level3_sort,
                "level4_sort": level4_sort,
                "person_sort": self.global_person_sort,  # Use global counter
                "division_name": division_name,
                "subdivision_name_initial": subdivision_name_initial,
                "subdivision_level2": cleaned_data.get("subdivision_level2", ""),
                "subdivision_level3": cleaned_data.get("subdivision_level3", ""),
                "subdivision_level4": cleaned_data.get("subdivision_level4", ""),
                "subdivision_name": cleaned_data.get("subdivision_name_final", ""),
                "person_name": person_name,
                "position_name": position_name,
                "person_phone": person_phone,
                "person_email": person_email,
                "person_fax": person_fax,
                "parent_org_id": None,
            }

            self.global_person_sort += 1 
            self.global_collected_items.append(data)
            self.logger.debug(f"Appended data to collected_items: {data}")
            # yield data  # Yield item immediately for Scrapy to process and log

        # Sort and yield items before making pagination requests
        if self.global_collected_items:
            sorted_items = self.sort_collected_data(self.global_collected_items)
            for item in sorted_items:
                yield item
            self.global_collected_items = []  # Clear collected items after yielding

        # Handle pagination
        pagination_links = response.css("ul.uk-pagination li a::attr(href)").getall()
        visited_pages = response.meta.get("visited_pages", set())
        visited_pages.add(response.url)

        if pagination_links:
            for link in pagination_links:
                next_page_url = response.urljoin(link)
                if next_page_url not in visited_pages:
                    print(F"\nNEXT PAGE TO SCRAPE: {next_page_url}")
                    yield scrapy.Request(
                        next_page_url,
                        callback=self.extract_person_details,
                        meta={
                            "division_name": division_name,
                            "visited_pages": visited_pages,
                        }
                    )

    def sort_collected_data(self, data):
        """
        Sorts the collected data hierarchically by LEVEL2, LEVEL3, and person_sort.
        """
        for item in data:
            if 'level2_sort' not in item or 'level3_sort' not in item or 'level4_sort' not in item:
                self.logger.debug(f"Missing sort keys in item: {item}")

        sorted_data = sorted(
            data,
            key=lambda x: (
                x['division_sort'],
                x.get('level2_sort', 0),
                x.get('level3_sort', 0),
                x.get('level4_sort', 0),
                x['person_sort']
            )
        )

        for idx, item in enumerate(sorted_data, start=1):
            item['position_sort'] = idx  

        return sorted_data

    def handle_request_error(self, failure):
        """Handle errors during requests."""
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(f"Failure details: {failure.value}")

    def clean_subdivision_name(self, division_name, subdivision_name_initial):
        """
        Clean and process the subdivision name to handle redundancy, hierarchy, and final formatting.

        Args:
            division_name (str): The name of the division.
            subdivision_name_initial (str): The raw subdivision name with potential redundancies.

        Returns:
            dict: Processed information with cleaned subdivision name and parent hierarchy.
        """
        subdivision_name = subdivision_name_initial.strip()
        subdivision_level2 = None
        subdivision_level3 = None
        subdivision_level4 = None

        print(f"\nStarting cleaning for: {subdivision_name_initial}")

        # Step 1: Remove division name from the raw subdivision name
        if division_name in subdivision_name:
            subdivision_name = subdivision_name.replace(division_name, "").strip(", ").strip()
            print(f"Removed division name: {division_name} -> {subdivision_name}")

        # Restriction to only map by division
        current_level2_to_level3_map = {
            k: v for k, v in self.level2_to_level3_map.items() if k.startswith(division_name)
        }
        
        # Step 2: Determine `subdivision_level2` (LEVEL2) using keys
        for level1_name, level1_children in self.level1_to_level2_map.items():
            if division_name == level1_name:  # Match LEVEL1
                for level2_key in level1_children:
                    level2_name = level2_key.split("::")[-1]  # Extract the LEVEL2 name from the key
                    if level2_name in subdivision_name:
                        subdivision_level2 = level2_key
                        subdivision_name = subdivision_name.replace(level2_name, "").strip(", ").strip()
                        print(f"Identified level2: {subdivision_level2}")
                        break

        # Step 3: Determine `subdivision_level3` (LEVEL3) based on `LEVEL2` using keys
        if subdivision_level2 and subdivision_level2 in current_level2_to_level3_map:
            for level3_key in current_level2_to_level3_map[subdivision_level2]:
                level3_name = level3_key.split("::")[-1]  # Extract the LEVEL3 name from the key
                if level3_name in subdivision_name:
                    subdivision_level3 = level3_key
                    subdivision_name = subdivision_name.replace(level3_name, "").strip(", ").strip()
                    print(f"Identified level3: {subdivision_level3}")
                    break
        else:
            # Step 4: Handle missing LEVEL2 when LEVEL3 is identified. 
            # Even if no level2 found, find out if there is level3. Then, solve the missing level2!
            for level2_key, level3_list in current_level2_to_level3_map.items():
                for level3_key in level3_list:
                    level3_name = level3_key.split("::")[-1]
                    if level3_name in subdivision_name:
                        subdivision_level3 = level3_key
                        subdivision_name = subdivision_name.replace(level3_name, "").strip(", ").strip()
                        subdivision_level2 = level2_key
                        print(f"Identified level3 (without level2): {subdivision_level3}")
                        print(f"Resolved missing level2: '{subdivision_level2}''")
                        break
                if subdivision_level3:
                    break

        # Step 5: Handle fallback normalization for LEVEL3
        if not subdivision_level3 and subdivision_name:
            for level2_key, level3_list in current_level2_to_level3_map.items():
                for level3_key in level3_list:
                    normalized_level3 = re.sub(r"[^\w\s]", "", level3_key.split("::")[-1]).lower().strip()
                    normalized_subdivision_name = re.sub(r"[^\w\s]", "", subdivision_name).lower().strip()

                    if normalized_level3 in normalized_subdivision_name:
                        print(f"Fallback level3 match found: '{level3_key}' in '{subdivision_name}'")
                        subdivision_name = subdivision_name.replace(level3_key.split("::")[-1], "").strip(", ").strip()
                        subdivision_level3 = level3_key
                        break
                if subdivision_level3:
                    break
                
        # Restriction to only map by division
        current_level3_to_level4_map = {
            k: v for k, v in self.level3_to_level4_map.items() if k.startswith(division_name)
        }

        # Step 6: Determine `subdivision_level4` (LEVEL4) based on `LEVEL3` using keys
        if subdivision_level3 and subdivision_level3 in current_level3_to_level4_map:
            for level4_key in current_level3_to_level4_map[subdivision_level3]:
                level4_name = level4_key.split("::")[-1]  # Extract the LEVEL4 name from the key
                if level4_name in subdivision_name:
                    subdivision_level4 = level4_key
                    subdivision_name = subdivision_name.replace(level4_name, "").strip(", ").strip()
                    print(f"Identified level4: {subdivision_level4}")
                    break
        else:
            # Step 7: Handle missing LEVEL3 when LEVEL4 is identified
            # Even if no level3 found, find out if there is level3. Then, solve the missing level3!
            for level3_key, level4_list in current_level3_to_level4_map.items():
                for level4_key in level4_list:
                    level4_name = level4_key.split("::")[-1]
                    if level4_name in subdivision_name:
                        subdivision_level4 = level4_key
                        subdivision_name = subdivision_name.replace(level4_name, "").strip(", ").strip()
                        subdivision_level3 = level3_key
                        print(f"Identified level4 (without level3): {subdivision_level4}")
                        print(f"Resolved missing level3: '{subdivision_level3}'")
                        break
                if subdivision_level4:
                    break

        # Step 8: Handle missing LEVEL2 when LEVEL3 is resolved
        # Check if the newly resolved `level3` has an associated `level2`
        if subdivision_level3 and not subdivision_level2:
            for level2_key, level3_list in current_level2_to_level3_map.items():
                if subdivision_level3 in level3_list:
                    subdivision_level2 = level2_key
                    print(f"Resolved missing level2: {subdivision_level2} for level3: {subdivision_level3}")
                    break

        # Step 9: Handle redundant names
        if subdivision_name and subdivision_level3 and subdivision_name in subdivision_level3.split("::")[-1]:
            print(f"Redundant name found: '{subdivision_name}' matches '{subdivision_level3}'")
            subdivision_name = None

        # Step 10: Final hierarchy cleanup and formatting
        subdivision_name_final = " > ".join(filter(None, [
            subdivision_level2.split("::")[-1] if subdivision_level2 else None,
            subdivision_level3.split("::")[-1] if subdivision_level3 else None,
            subdivision_level4.split("::")[-1] if subdivision_level4 else None,
            subdivision_name
        ]))

        subdivision_name_final = subdivision_name_final.strip() if subdivision_name_final.strip() else None

        print(f"Final cleaned name: {subdivision_name_final}")

        return {
            "subdivision_level2": subdivision_level2,
            "subdivision_level3": subdivision_level3,
            "subdivision_level4": subdivision_level4,
            "subdivision_name_final": subdivision_name_final
        }
