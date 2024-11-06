import scrapy
import json
from scrapy_playwright.page import PageMethod

class RURALLINK_AnggotaSpider(scrapy.Spider):
    name = "rurallink_anggota"

    #manual mapping of division names to sort order as seen on website (unknown division will be set as 999)
    division_sort_mapping = {
        "Pejabat YB Menteri": 1,
        "Pejabat YB Timbalan Menteri": 2,
        "Pejabat KSU": 3,
        "Pejabat KKDW Negeri Selangor/WP Kuala Lumpur/WP Putrajaya/WP Labuan": 4,
        "Pejabat KKDW Negeri Pahang": 5,
        "Pejabat KKDW Negeri Johor": 6,
        "Pejabat KKDW Negeri Sembilan": 7,
        "Pejabat KKDW Negeri Melaka": 8,
        "Pejabat KKDW Negeri Perak": 9,
        "Pejabat KKDW Negeri Kelantan": 10,
        "Pejabat KKDW Negeri Terengganu": 11,
        "Pejabat KKDW Negeri Kedah/Perlis/Pulau Pinang": 12,
        "Pejabat KKDW Negeri Sarawak": 13,
        "Pejabat KKDW Negeri Sabah": 14,
        "Pejabat TKSU Dasar": 15,
        "Pejabat TKSU Pembangunan": 16,
        "Pejabat Setiausaha Bahagian Kanan (Khidmat Pengurusan)": 17,
        "Bahagian Prasarana": 18,
        "Bahagian Teknikal": 19,
        "Bahagian Penyelarasan dan Pemantauan": 20,
        "Bahagian Perancangan Strategik": 21,
        "Bahagian Pembangunan Usahawan Desa": 22,
        "Bahagian Pengurusan Sumber Manusia": 23,
        "Bahagian Akaun": 24,
        "Bahagian Kewangan": 25,
        "Bahagian Pentadbiran dan Pengurusan Aset": 26,
        "Bahagian Komuniti Desa": 27,
        "Bahagian Pengurusan Maklumat": 28,
        "Bahagian Kesejahteraan Rakyat": 29,
        "Bahagian Kemajuan Tanah dan Wilayah": 30,
        "Bahagian Korporat dan Pembangunan Kemahiran": 31,
        "Bahagian Perolehan": 32,
        "Bahagian Pembangunan Ekonomi Komuniti": 33,
        "Unit Perundangan": 34,
        "Unit Audit Dalam": 35,
        "Unit Komunikasi Korporat": 36,
        "Unit Integriti": 37
    }

    person_sort_order = 0

    def start_requests(self):
        url = 'https://www.rurallink.gov.my/direktori-anggota/'

        yield scrapy.Request(
            url,
            callback=self.parse,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_selector", "table"),
                ],
            },
            errback=self.errback_httpbin
        )

    async def parse(self, response):
        page = response.meta["playwright_page"]
        
        try:
            wdt_nonce = await page.evaluate('''() => document.querySelector('input[name="wdtNonceFrontendServerSide_58"]').value''')

            payload = {
                "draw": 5,
                "columns": [
                    {"data": 0, "name": "ordering", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 1, "name": "wdt_ID", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 2, "name": "gambar", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 3, "name": "bahagianunit", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 4, "name": "nama", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 5, "name": "jawatan", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 6, "name": "sambungan", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                    {"data": 7, "name": "emelrurallinkgovmy", "searchable": "true", "orderable": "true", "search": {"value": "", "regex": "false"}},
                ],
                "order": [{"column": 0, "dir": "asc"}],
                "start": 0,
                "length": -1,
                "search": {"value": "", "regex": "false"},
                "wdtNonce": wdt_nonce,
                "sRangeSeparator": "|"
            }

            ajax_url = 'https://www.rurallink.gov.my/wp-admin/admin-ajax.php?action=get_wdtable&table_id=58'
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
                self.logger.warning("No data returned")
                return

            for item in self.parse_ajax_data(ajax_response['data']): #process data
                yield item

        except Exception as e:
            self.logger.error(f"Error in parse method: {str(e)}")
        finally:
            await page.close()

    def parse_ajax_data(self, data):
        for row in data:
            self.person_sort_order += 1 
            division_name = row[3] if row[3] else None

            #lookup the division_sort_order using the division name
            division_sort_order = self.division_sort_mapping.get(division_name, 999999)  #default to 999 if not found

            email = row[7].strip() if row[7] else None

            if email:
                email = email.replace('[at]', '@').replace('[dot]', '.').replace('[.]', '.').replace('[@]', '@')
                email = email.replace('[com]', '.com').replace('[my]', '.my')

                email = email.strip().replace(" ", "")

                if email.startswith('@'):
                    email = None
                elif "@" in email and "." in email.split("@")[-1]:
                    pass
                else:
                    email = f"{email}@rurallink.gov.my"

            yield {
                'org_sort': 999,
                'org_id': 'RURALLINK',
                'org_name': 'KEMENTERIAN KEMAJUAN DESA DAN WILAYAH',
                'org_type': 'ministry',
                'division_sort': division_sort_order,
                'position_sort_order': self.person_sort_order,
                'division_name': division_name.strip() if division_name else None,
                'subdivision_name': None,
                'person_name': row[4].strip() if row[4] else None,
                'position_name': row[5].strip() if row[5] else None,
                'person_phone': row[6].strip() if row[6] else None,
                'person_email': email.strip() if email else None,
                'person_fax': None,
                'parent_org_id': None, #is the parent
            }

    async def errback_httpbin(self, failure):
        page = failure.request.meta["playwright_page"]
        await page.close()
        self.logger.error(f"Error occurred: {failure.value}")
