import scrapy
import re

class MOFSpider(scrapy.Spider):
    name = 'mof'

    #manual mapping of division names to sort order
    division_sort_mapping = {
        "Pejabat Menteri, Timbalan Menteri dan Pegawai Atasan": 1,
        "Bahagian Antarabangsa (INT)": 2,
        "Bahagian Cukai (TAX)": 3,
        "Bahagian Dasar Saraan dan Pengurusan (RPM)": 4,
        "Bahagian Fiskal dan Ekonomi (FED)": 5,
        "Bahagian Pelaburan Strategik (SID)": 6,
        "Bahagian Pengurusan Aset Awam (PAM)": 7,
        "Bahagian Pengurusan Strategik Badan Berkanun (SBM)": 8,
        "Bahagian Perolehan Kerajaan (GPD)": 9,
        "Bahagian Kawalan Kewangan Strategik dan Korporat (BKSK)": 10,
        "Bahagian Syarikat Pelaburan Kerajaan (GIC)": 11,
        "Bahagian Teknologi Maklumat (ITD)": 12,
        "Bahagian Undang-Undang (BUU)": 13,
        "Pejabat Belanjawan Negara (NBO)": 14,
        "Pejabat Pendaftar Agensi Pelaporan Kredit (PPK)": 15,
        "Pejabat Pesuruhjaya Khas Cukai Pendapatan (PKCP)": 16,
        "Tribunal Rayuan Kastam (TRK)": 17,
        "Unit Audit Dalam Perbendaharaan (ADP)": 18,
        "Unit Integriti (UI)": 19,
        "Unit Pantau Madani": 20,
        "Unit Komunikasi Korporat (UKK)": 21,
        #"Perbendaharaan Malaysia Sabah": 22, #website is different
        #"Perbendaharaan Malaysia Sarawak":23, #website is different
    }

    person_sort_order = 0

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
        'https://www.mof.gov.my/portal/ms/hubungi/direktori/unit-komunikasi-korporat',
        #'https://pmsabah.treasury.gov.my/index.php/ms/hubungi-kami', #website is different
        #'http://pmsarawak.treasury.gov.my/?page_id=46' #website is different
    ]

    def parse(self, response):

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

        #determine the division sort order using the mapping
        division_sort_order = self.division_sort_mapping.get(division, 999)  #use 999 as default if not found

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
                'division_sort': division_sort_order,
                'person_sort_order': self.person_sort_order,
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
