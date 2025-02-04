import scrapy
import re

class MOFSpider(scrapy.Spider):
    name = 'mof'
    allowed_domains = ["www.mof.gov.my"]
    start_urls = ["https://www.mof.gov.my/portal/ms/hubungi/direktori"]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    person_sort_order = 0

    def parse(self, response):
        division_sort = 0
        excluded_hrefs = ['https://pmsabah.treasury.gov.my/index.php/ms/hubungi-kami', 
                          'http://pmsarawak.treasury.gov.my/?page_id=46',
                          'https://www.mof.gov.my/portal/ms/hubungi/direktori/ppk',
                          ]
        
        divisions = response.css(".sppb-addon-title.sppb-feature-box-title a")
        for division in divisions:
            href = division.attrib.get("href")
            division_name = division.css("::text").get().strip()
        
            if href in excluded_hrefs:
                continue

            division_sort += 1

            self.logger.debug(f"\nDivision_name: {division_name}, {division_sort}")

            yield scrapy.Request(
                url=response.urljoin(href),
                callback=self.parse_item,
                meta={'division_name': division_name, 'division_sort': division_sort}
                )

    def parse_item(self, response):
        division_sort = response.meta.get('division_sort')  
        division = response.css('h2::text').get(default='').strip()
        division_address_block = response.xpath('//div[@class="category-desc"]//p/text()[normalize-space()]').getall()
        division_address = ' '.join([line.strip() for line in division_address_block if not any(keyword in line for keyword in ['Tel', 'Fax', ':', 'Tekan'])])
        division_address = re.sub(r'\s+', ' ', division_address.replace(u'\xa0', ' ')).strip()

        division_fax = response.xpath("//strong[contains(text(), 'Fax')]/following-sibling::text()").get()
        if division_fax:
            division_fax = division_fax.strip().replace(':', '').replace(' ', '').strip()
            division_fax = division_fax.replace(u'\xa0', '').strip()

        division_phone = response.xpath('//p/strong[contains(text(), "Tel")]/following-sibling::text()[1]').get()
        if division_phone:
            division_phone = division_phone.strip().replace(':', '').replace(' ', '').strip()

        current_unit = None
        unit_address = None
        unit_phone = None

        #iterate row
        rows = response.css('table.table tbody tr')
        for row in rows:
            #check if the row is a unit header (th colspan="2")
            header = row.css('th[colspan="2"] h2::text').get()
            if header:
                #if a new unit header is found, update the current_unit
                current_unit = header.strip()

                #extract unit address and phone number from the same header block
                unit_address = row.css('th[colspan="2"]::text').getall()[1].strip() if len(row.css('th[colspan="2"]::text').getall()) > 1 else None
                unit_phone = row.xpath('.//i[@class="fa fa-print"]/following-sibling::text()[1]').get(default='').strip()
                
                continue  #move to the next row after setting the "unit" value

            self.person_sort_order += 1

            person_name = row.css('td strong::text').get(default='').strip()
            person_position = row.css('td::text').getall()[1].strip() if len(row.css('td::text').getall()) > 1 else ''
            
            person_email = row.xpath('.//i[@class="fa fa-envelope"]/following-sibling::text()[1]').get(default='').strip()
            person_phone = row.xpath('.//i[@class="fa fa-phone-alt"]/following-sibling::text()[1]').get(default='').strip()

            if person_email:
                person_email = person_email.replace('[at]', '@').replace('[dot]', '.').replace('[.]', '.').replace('[@]', '@')

                if "@" in person_email:
                    if not person_email.endswith(".gov.my"): #person@mof
                        person_email = f"{person_email}.gov.my" 
                elif "@mof.gov.my" in person_email:
                    pass
                else:
                    person_email = f"{person_email}@mof.gov.my"


            yield {
                'org_sort': 999,
                'org_id': "MOF",
                'org_name': "KEMENTERIAN KEWANGAN MALAYSIA",
                'org_type': 'ministry',
                'division_sort': division_sort,
                'position_sort_order': self.person_sort_order,
                'division_name': division if division else None,
                #'division_address': division_address if division_address else None,
                #'division_phone': division_phone if division_phone else None,
                #'division_fax': division_fax if division_fax else None,
                'subdivision_name': current_unit if current_unit else None,
                #'unit_address': unit_address if unit_address else None,
                'person_name': person_name if person_name else None,
                'person_phone': person_phone if person_phone else None,
                'position_name': person_position if person_position else None,
                'person_email': person_email if person_email else None,
                'person_fax': None,
                'parent_org_id': None, #is the parent

            }
