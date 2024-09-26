import scrapy
import json
from scrapy_playwright.page import PageMethod
import logging

class KKWD_PKDSpider(scrapy.Spider):
    name = "kkdw_pkd"
    
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
            total_records = await page.evaluate('''
                () => {
                    const info = document.querySelector('.dataTables_info');
                    if (info) {
                        const match = info.textContent.match(/of (\d+) entries/);
                        return match ? parseInt(match[1]) : 0;
                    }
                    return 0;
                }
            ''')

            self.logger.info(f"Total records found: {total_records}")

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
            yield {
                'agency_id': "RURALLINK",
                'agency': 'KEMENTERIAN KEMAJUAN DESA DAN WILAYAH',
                #'no': row[0],
                #'wdt_ID': row[1],
                'division': "Pusat Komuniti Desa (PKD)",
                'unit': f"{row[2]} ({row[6]})",
                #'photo': self.extract_image_url(row[3]),
                'person_name': row[4],
                'person_position': row[5],
                #'personal_phone': row[7],
                'person_phone': row[8],
                'person_email': self.extract_email(row[9]),
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