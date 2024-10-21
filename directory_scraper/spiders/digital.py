import scrapy
from scrapy_playwright.page import PageMethod
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

class DIGITALSpider(scrapy.Spider):
    name = 'digital'
    start_urls = ['https://www.digital.gov.my/direktori']

    person_sort_order = 0
    division_sort_order = 0
    last_processed_division = None

    custom_settings = {
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "headless": True,
        }
    }

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "table")
                    ]
                },
                callback=self.parse
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        page_count = 0
        current_unit = None
        current_division = None
        all_data = []

        while True:
            page_count += 1
            rows = response.xpath('//table/tbody/tr')

            if not rows:
                self.logger.error("No rows found in the table!")
            else:
                print(f"Found {len(rows)} rows in page {page_count}.")

            for row in rows:
                name = row.xpath('.//td[contains(@id, "_nama")]/text()').get()
                division_parts = row.xpath('.//td[contains(@id, "_bhg")]//p//text()').getall()
                division = ''.join(division_parts).strip() if division_parts else None
                division = division.replace("...","") if division else None
                position = row.xpath('.//td[contains(@id, "_jawatan")]/p/text()').get()
                phone = row.xpath('.//td[contains(@id, "_telefon")]/p/text()').get()
                email = row.xpath('.//td[contains(@id, "_emel")]/p/text()').get()

                if division != self.last_processed_division:
                    self.division_sort_order += 1 
                    self.last_processed_division = division

                if email and email not in ["-", "—", "–"]:
                    email = f"{email}@digital.gov.my"

                if division and division != current_division:
                    current_division = division
                    if name:  # If it's a person (must have name value)
                        current_unit = None  # Reset the current unit when the division changes
                    #print(f"Detected new division: {current_division}")

                # If name exists and all other fields are null, it's a unit
                if name and not division and not position and not phone and not email:
                    current_unit = name  # Set this as the current unit
                    #print(f"Detected unit: {current_unit}")
                    continue  # Skip to the next row, as this is a unit

                self.person_sort_order += 1

                staff_data = {
                    'org_sort': 999,
                    'org_id': 'DIGITAL',
                    'org_name': 'KEMENTERIAN DIGITAL',
                    'org_type': 'ministry',
                    'division_sort': self.division_sort_order,
                    'division_name': division.strip() if division else None,
                    'subdivision_name': current_unit.strip() if current_unit else None,
                    'position_sort_order': self.person_sort_order,
                    'position_name': position.strip() if position else None,
                    'person_name': name.strip() if name else None,
                    'person_phone': phone.strip() if phone else None,
                    'person_email': email.strip() if email else None,
                    'person_fax': None,
                    'parent_org_id': None,  # is the parent
                    #'page_number': page_count
                }

                all_data.append(staff_data)

            try:
                # Check if the "Next" button is available and clickable
                next_button = await page.query_selector('button[aria-label="Seterusnya"]:not([disabled])')

                if next_button:
                    print(f"Clicking the 'Seterusnya' (Next) button to load page {page_count + 1}...")
                    await next_button.click()
                    await page.wait_for_selector('table')  # Wait for the next page's table to load
                    #await page.wait_for_timeout(2000)  # delay to ensure the table loads fully
                    new_body = await page.content()
                    response = scrapy.http.HtmlResponse(
                        url=response.url,
                        body=new_body,
                        encoding='utf-8',
                        request=response.request
                    )
                else:
                    break

            except PlaywrightTimeoutError:
                # Handle case where clicking the next button fails due to visibility or timeout issues
                #print(f"TimeoutError: Unable to click the 'Seterusnya' button on page {page_count}. Ending scraping.")
                break

        await page.close()

        # post-processing after scraping is complete
        fixed_data = fix_subdivision_value(all_data)
        cleaned_data = delete_person_null(fixed_data)
        cleaned_data = replace_dash_with_none(cleaned_data)

        for item in cleaned_data:
            yield item

def fix_subdivision_value(data):
    """Function to fix the subdivision_name based on the last valid entry (subdivision_name is null due to pagination)"""
    last_valid_obj = None  # Keep track of the last valid object with a subdivision_name

    for i, obj in enumerate(data):
        # Skip objects with null person_name as per your request
        if not obj['person_name']:
            continue

        # If the object has a valid subdivision_name, update last_valid_obj
        if obj['subdivision_name']:
            last_valid_obj = obj
        else:
            # If the current object has the same division_name as last_valid_obj, inherit the subdivision_name
            if last_valid_obj and obj['division_name'] == last_valid_obj['division_name']:
                obj['subdivision_name'] = last_valid_obj['subdivision_name']

    return data

def delete_person_null(data):
    """Filter out any object where person_name is None or an empty string. 
    notes:
    1. if person_name is None, its just an empty objects (dirty data)
    2. if person_name is "-", its an empty position (valid data) """
    return [obj for obj in data if obj['person_name']]

def replace_dash_with_none(data):
    """Cleanup function to standardize final outputs with other spiders. 
    To replace "-" with None. 
    Can only be run after delete_person_null() to avoid deleting relevant/valid data before doing standardization.
    """
    for obj in data:
        if obj.get('person_name') in ["-", "—", "–"]:  # Check for hyphen, em dash, en dash
            obj['person_name'] = None        
        if obj.get('person_email') in ["-", "—", "–"]:  # Check for hyphen, em dash, en dash
            obj['person_email'] = None
        if obj.get('person_phone') in ["-", "—", "–"]:  # Check for hyphen, em dash, en dash
            obj['person_phone'] = None

    return data