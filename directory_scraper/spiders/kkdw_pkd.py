import scrapy
import json
from scrapy_playwright.page import PageMethod
import logging

class KKDW_PKDSpider(scrapy.Spider):
    name = "kkdw_pkd"
    
    seen_divisions = {} #init
    division_sort_counter = 1  #init (start w/ 1)
    person_sort_order = 0 #init global

    def start_requests(self):
        url = 'https://www.rurallink.gov.my/direktori-pkd/'
        yield scrapy.Request(
            url, 
            callback=self.parse, 
            meta={
                "playwright": True, 
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "table"),
                ],
                "errback": self.errback_httpbin,
            }
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        try:
            #get total number of records
            total_records = await page.evaluate(r'''
                () => {
                    const info = document.querySelector('.dataTables_info');
                    if (info) {
                        const match = info.textContent.match(/of (\d+) entries/);
                        return match ? parseInt(match[1]) : 0;
                    }
                    return 0;
                }
            ''')

            #calculate number of pages (assume 10 records per page as of that website)
            num_pages = -(-total_records // 10)

            wdt_nonce = await page.evaluate('''
                    () => document.querySelector('input[name="wdtNonceFrontendServerSide_106"]').value
                    ''')

            for page_num in range(num_pages):
                ajax_url = 'https://www.rurallink.gov.my/wp-admin/admin-ajax.php?action=get_wdtable&table_id=106'
                
                payload = {
                    "draw": page_num + 1,
                    "columns": [
                        {"data": i, "name": col_name, "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}}
                        for i, col_name in enumerate(["bil", "wdt_ID", "negeri", "gambarpengurus", "namapengurus", "jawatan", "pkd", "notelefon", "notelefonpejabat", "emel"])
                    ],
                    "order": [{"column": 0, "dir": "asc"}],
                    "start": page_num * 10,
                    "length": 10,
                    "search": {"value": "", "regex": "false"},
                    "wdtNonce": wdt_nonce,
                    "sRangeSeparator": "|"
                }

                ajax_response = await page.evaluate(f'''
                    async () => {{
                        const response = await fetch('{ajax_url}', {{
                            method: 'POST',
                            headers: {{
                                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                                'X-Requested-With': 'XMLHttpRequest'
                            }},
                            body: new URLSearchParams({json.dumps(payload)}).toString()
                        }});
                        return await response.json();
                    }}
                ''')

                if not ajax_response or not ajax_response.get('data'):
                    self.logger.warning(f"No data returned for page {page_num + 1}")
                    break

                for item in self.parse_ajax_data(ajax_response['data']):
                    yield item

        except Exception as e:
            self.logger.error(f"Error in parse method: {str(e)}")

        finally:
            await page.close()

    def parse_ajax_data(self, data):
        for row in data:
            division_name = row[2]
            
            # Check if this is a new division
            if division_name not in self.seen_divisions:
                self.seen_divisions[division_name] = self.division_sort_counter
                division_sort_order = self.division_sort_counter
                self.division_sort_counter += 1
            else:
                division_sort_order = self.seen_divisions[division_name]

            self.person_sort_order += 1

            yield {
                'org_sort': 999,
                'org_id': "RURALLINK",
                'org_name': 'KEMENTERIAN KEMAJUAN DESA DAN WILAYAH',
                'org_type': 'ministry',
                'division_sort': division_sort_order,
                'person_sort_order': self.person_sort_order,
                #'no': row[0],
                #'wdt_ID': row[1],
                'division_name': f"Pusat Komuniti Desa {row[2]}" if row[2] else f"Pusat Komuniti Desa",
                'subdivision_name': row[6] if row[6] else None, 
                #'photo': self.extract_image_url(row[3]),
                'person_name': row[4] if row[4] else None,
                'position_name': row[5] if row[5] else None,
                #'personal_phone': row[7] if row[7] else None,
                'person_phone': row[8] if row[8] else None,
                'person_email': self.extract_email(row[9]) if row[9] else None,
                'person_fax': None,
                'parent_org_id': None, 
            }

    def extract_image_url(self, html_string):
        import re
        match = re.search(r'src=[\'"]?([^\'" >]+)', html_string)
        return match.group(1) if match else None

    def extract_email(self, html_string):
        import re
        match = re.search(r'mailto:([^\'"]+)', html_string)
        return match.group(1) if match else None

    async def errback_httpbin(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
        self.logger.error(f"Error occurred: {failure.value}")
