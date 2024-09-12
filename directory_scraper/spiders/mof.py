import scrapy
import re

class MOFSpider(scrapy.Spider):
    name = 'mof'
    start_urls = [
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/menteri-dan-pegawai-atasan',
        'https://www.mof.gov.my/portal/index.php/hubungi/direktori/int',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/tax',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/rpm',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/ed',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/sid',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/pam',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/sbm',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/gpd',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/bksk',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/gic',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/itd',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/buu',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/nbo',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/ppk',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/pkcp',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/trk',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/adp',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/ui',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/madani',
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/unit-komunikasi-korporat'
    ]

    def parse(self, response):
        agency = "KEMENTERIAN KEWANGAN MALAYSIA"

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

        current_unit = None  # initialize
        unit_address = None  # initialize
        unit_phone = None  # initialize

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

            person_name = row.css('td strong::text').get(default='').strip()
            person_position = row.css('td::text').getall()[1].strip() if len(row.css('td::text').getall()) > 1 else ''
            
            person_email = row.xpath('.//i[@class="fa fa-envelope"]/following-sibling::text()[1]').get(default='').strip()
            person_phone = row.xpath('.//i[@class="fa fa-phone-alt"]/following-sibling::text()[1]').get(default='').strip()

            yield {
                'agency': agency,
                'division': division,
                #'division_address': division_address,
                #'division_phone': division_phone,
                #'division_fax': division_fax,
                'unit': current_unit,  
                #'unit_address': unit_address if unit_address else None,  #store None if missing
                #'unit_phone': unit_phone if unit_phone else None,
                'person_name': person_name,
                'person_phone': person_phone if person_phone else None,  #store None if missing
                'person_position': person_position,
                'person_email': person_email if person_email else None  #store None if missing
                }
