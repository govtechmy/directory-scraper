import scrapy
import re

class NRESSpider(scrapy.Spider):
    name = 'nres'

    person_counter = 0

    start_urls = ['https://www.nres.gov.my/_layouts/ketsaportal/VbForm/Direktori/DirektoriBio.aspx']

    #manually map the "unit_name" with its relevant "division_name"
    division_unit_mapping = {
    # "Bahagian Perancangan Strategik Dan Hubungan Antarabangsa (BPSA)": "PEJABAT KETUA SETIAUSAHA",
    # "Unit Audit Dalam (UAD)": "PEJABAT KETUA SETIAUSAHA",
    # "Pejabat Penasihat Undang-Undang": "PEJABAT KETUA SETIAUSAHA",
    # "Unit Komunikasi Korporat (UKK)": "PEJABAT KETUA SETIAUSAHA",
    # "Unit Integriti (UI)": "PEJABAT KETUA SETIAUSAHA",
    # "PEJABAT SETIAUSAHA BAHAGIAN TANAH, UKUR DAN GEOSPATIAL (BTUG)": "PEJABAT TIMBALAN KETUA SETIAUSAHA (SUMBER ASLI) - TKSU (SA)",
    "Seksyen Tanah": "PEJABAT SETIAUSAHA BAHAGIAN TANAH, UKUR DAN GEOSPATIAL (BTUG)",
    "Seksyen Ukur dan Pemetaan": "PEJABAT SETIAUSAHA BAHAGIAN TANAH, UKUR DAN GEOSPATIAL (BTUG)",
    "Pusat Geospatial Negara (PGN)": "PEJABAT SETIAUSAHA BAHAGIAN TANAH, UKUR DAN GEOSPATIAL (BTUG)",
    # "PEJABAT SETIAUSAHA BAHAGIAN MINERAL DAN GEOSAINS (BMG)": "PEJABAT TIMBALAN KETUA SETIAUSAHA (SUMBER ASLI) - TKSU (SA)",
    "Seksyen Mineral": "PEJABAT SETIAUSAHA BAHAGIAN MINERAL DAN GEOSAINS (BMG)",
    "Seksyen Geosains": "PEJABAT SETIAUSAHA BAHAGIAN MINERAL DAN GEOSAINS (BMG)",
    # "BAHAGIAN PENGURUSAN BIODIVERSITI DAN PERHUTANAN": "PEJABAT TIMBALAN KETUA SETIAUSAHA (SUMBER ASLI) - TKSU (SA)",
    "Seksyen Pengurusan Biodiversiti": "BAHAGIAN PENGURUSAN BIODIVERSITI DAN PERHUTANAN",
    "Seksyen Pengurusan Perhutanan": "BAHAGIAN PENGURUSAN BIODIVERSITI DAN PERHUTANAN",
    # "BAHAGIAN PENGURUSAN ALAM SEKITAR (BPAS)": "PEJABAT TIMBALAN KETUA SETIAUSAHA (KELESTARIAN ALAM) - TKSU (KA)",
    "Seksyen Pengurusan Sumber Alam": "BAHAGIAN PENGURUSAN ALAM SEKITAR (BPAS)",
    "Seksyen Dasar Alam Sekitar": "BAHAGIAN PENGURUSAN ALAM SEKITAR (BPAS)",
    # "BAHAGIAN PERUBAHAN IKLIM (BPI)": "PEJABAT TIMBALAN KETUA SETIAUSAHA (KELESTARIAN ALAM) - TKSU (KA)",
    "Seksyen Dasar Perubahan Iklim": "BAHAGIAN PERUBAHAN IKLIM (BPI)",
    "Seksyen Dasar Inisiatif Lestari": "BAHAGIAN PERUBAHAN IKLIM (BPI)",
    "Pusat Gas Rumah Kaca Kebangsaan": "BAHAGIAN PERUBAHAN IKLIM (BPI)",
    # "UNIT REDD PLUS": "PEJABAT TIMBALAN KETUA SETIAUSAHA (KELESTARIAN ALAM) - TKSU (KA)",
    # "Bahagian Khidmat Pengurusan": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    # "Bahagian Pengurusan Sumber Manusia": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    # "Bahagian Kewangan Dan Perolehan": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    # "Bahagian Pengurusan Maklumat (BPM)": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    "Seksyen Perancangan & Pengurusan": "Bahagian Pengurusan Maklumat (BPM)",
    "Seksyen Pembangunan Sistem": "Bahagian Pengurusan Maklumat (BPM)",
    "Seksyen Khidmat Teknikal & Operasi": "Bahagian Pengurusan Maklumat (BPM)",
    # "Bahagian Pembangunan (BP)": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    # "Bahagian Akaun (BA)": "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK",
    }

    # Division sort order mapping based on the dropdown sequence
    division_sort_order_mapping = {
        "PEJABAT MENTERI": 1,
        "PEJABAT TIMBALAN MENTERI": 2,
        "PEJABAT KETUA SETIAUSAHA": 3,
        "Bahagian Perancangan Strategik Dan Hubungan Antarabangsa (BPSA)": 4,
        "Unit Audit Dalam (UAD)": 5,
        "Pejabat Penasihat Undang-Undang": 6,
        "Unit Komunikasi Korporat (UKK)": 7,
        "Unit Integriti (UI)": 8,
        "PEJABAT TIMBALAN KETUA SETIAUSAHA (SUMBER ASLI) - TKSU (SA)": 9,
        "PEJABAT SETIAUSAHA BAHAGIAN TANAH, UKUR DAN GEOSPATIAL (BTUG)": 10,
        "Seksyen Tanah": 11,
        "Seksyen Ukur dan Pemetaan": 12,
        "Pusat Geospatial Negara (PGN)": 13,
        "PEJABAT SETIAUSAHA BAHAGIAN MINERAL DAN GEOSAINS (BMG)": 14,
        "Seksyen Mineral": 15,
        "Seksyen Geosains": 16,
        "BAHAGIAN PENGURUSAN BIODIVERSITI DAN PERHUTANAN": 17,
        "Seksyen Pengurusan Biodiversiti": 18,
        "Seksyen Pengurusan Perhutanan": 19,
        "PEJABAT TIMBALAN KETUA SETIAUSAHA (KELESTARIAN ALAM) - TKSU (KA)": 20,
        "BAHAGIAN PENGURUSAN ALAM SEKITAR (BPAS)": 21,
        "Seksyen Pengurusan Sumber Alam": 22,
        "Seksyen Dasar Alam Sekitar": 23,
        "BAHAGIAN PERUBAHAN IKLIM (BPI)": 24,
        "Seksyen Dasar Perubahan Iklim": 25,
        "Seksyen Dasar Inisiatif Lestari": 26,
        "Pusat Gas Rumah Kaca Kebangsaan": 27,
        "UNIT REDD PLUS": 28,
        "PEJABAT SETIAUSAHA BAHAGIAN KANAN (PENGURUSAN) - SUBK": 29,
        "Bahagian Khidmat Pengurusan": 30,
        "Bahagian Pengurusan Sumber Manusia": 31,
        "Bahagian Kewangan Dan Perolehan": 32,
        "Bahagian Pengurusan Maklumat (BPM)": 33,
        "Seksyen Perancangan & Pengurusan": 34,
        "Seksyen Pembangunan Sistem": 35,
        "Seksyen Khidmat Teknikal & Operasi": 36,
        "Bahagian Pembangunan (BP)": 37,
        "Bahagian Akaun (BA)": 38
    }

    def parse(self, response):
        # Extract hidden fields required for form submission
        viewstate = response.xpath('//input[@name="__VIEWSTATE"]/@value').get()
        viewstategenerator = response.xpath('//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
        eventvalidation = response.xpath('//input[@name="__EVENTVALIDATION"]/@value').get()

        if not viewstate or not viewstategenerator or not eventvalidation:
            self.log("One or more required form fields not found.", level=scrapy.log.ERROR)
            return

        #extract all division options from the dropdown
        divisions = response.xpath('//select[@name="ddlBahagian"]/option/@value').getall()

        # Loop through each division and create a form request for each
        for division in divisions:
            if 'SILA PILIH' in division:
                continue  #skip the placeholder option

            form_data = {
                '__EVENTTARGET': 'ddlBahagian',
                '__EVENTARGUMENT': '',
                '__LASTFOCUS': '',
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewstategenerator,
                '__EVENTVALIDATION': eventvalidation,
                'ddlBahagian': division,  #set each division in the form payload
                'tbAuthor': '',
            }

            yield scrapy.FormRequest(
                url='https://www.nres.gov.my/_layouts/ketsaportal/VbForm/Direktori/DirektoriBio.aspx',
                formdata=form_data,
                callback=self.parse_results,
                meta={'division': division}  # Pass division to the callback
            )
            
    def parse_results(self, response):
        rows = response.xpath('//table[@id="gvSearchResults"]/tr[position() > 1]')  # Skip header row
        division_name = response.meta['division']  # Get division name from the request metadata

        if not rows:
            self.log(f"No rows found for division: {division_name}.", level=scrapy.log.ERROR)
            return
        
        for row in rows:
            person_name = row.xpath('td[1]/text()').get()
            bahagian = row.xpath('td[2]/text()').get()
            person_position = row.xpath('td[3]/text()').get()
            person_email = row.xpath('td[4]/text()').get()  # Modify if email column exists
            person_phone = row.xpath('td[5]/text()').get()  # Modify if phone column exists

            #clean and normalize the bahagian (not division_name)
            cleaned_bahagian = re.sub(r'\s+', ' ', bahagian.replace('\xa0', ' ').replace('—', '').strip())

            #check if the bahagian is actually a unit (if found in the mapping)
            if cleaned_bahagian in self.division_unit_mapping:
                unit_name = cleaned_bahagian  #set current bahagian as "unit_name"
                division_name = self.division_unit_mapping[cleaned_bahagian]  #map to correct "division_name"
            else:
                unit_name = None  # If not a unit, leave it as None
            
            final_division_name = re.sub(r'\s+', ' ', division_name.replace('\xa0', ' ').replace('—', '').strip())
            final_division_name = re.sub(r'^•\s*', '', final_division_name).strip()

            division_sort_order = self.division_sort_order_mapping.get(final_division_name, 999)

            self.person_counter += 1
            person_sort_order = self.person_counter


            yield {
                'org_sort': 999,
                'org_id': 'NRES',
                'org_name': 'KEMENTERIAN SUMBER ASLI DAN KELESTARIAN ALAM',
                'org_type': 'ministry',
                'division_sort': division_sort_order,  # Use the mapped sort order
                'person_sort_order': person_sort_order,  # Use the global person sort order
                'division_name':  final_division_name, #re.sub(r'^•\s*', '', re.sub(r'\s+', ' ', division_name.replace('\xa0', ' ').replace('—', '').strip())).strip(), #division_name,#cleaned_division_name, #re.sub(r'^•\s*', '', cleaned_division_name).strip(),
                #'bahagian': bahagian,
                'unit_name': unit_name,
                'person_name': person_name,
                'person_position': person_position,
                'person_email': f"{person_email}@nres.gov.my",
                'person_phone': person_phone,
                'person_fax': None,  
                'parent_org_id': None,  

            }