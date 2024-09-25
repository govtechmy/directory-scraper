import scrapy

class MotSpider(scrapy.Spider):
    name = 'mot'
    start_urls = [
        'https://www.mot.gov.my/my/directory/staff?bahagian=Pejabat%20Menteri&pagetitle=Pejabat%20Menteri',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Pejabat%20Timbalan%20Menteri&pagetitle=Pejabat%20Timbalan%20Menteri',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Pejabat%20Ketua%20Setiausaha&pagetitle=Pejabat%20Ketua%20Setiausaha',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Biro%20Siasatan%20Kemalangan%20Udara&pagetitle=Biro%20Siasatan%20Kemalangan%20Udara',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Perolehan%20Dan%20Kewangan&pagetitle=Bahagian%20Perolehan%20Dan%20Kewangan',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Akaun&pagetitle=Bahagian%20Akaun',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Perancangan%20Strategik%20%26%20Antarabangsa&pagetitle=Bahagian%20Perancangan%20Strategik%20%26%20Antarabangsa',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Maritim&pagetitle=Bahagian%20Maritim',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Darat&pagetitle=Bahagian%20Darat',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Udara&pagetitle=Bahagian%20Udara',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Pembangunan&pagetitle=Bahagian%20Pembangunan',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Pengurusan%20Maklumat&pagetitle=Bahagian%20Pengurusan%20Maklumat',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Unit%20Komunikasi%20Korporat&pagetitle=Unit%20Komunikasi%20Korporat',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Unit%20Penasihat%20Undang-undang&pagetitle=Unit%20Penasihat%20Undang-undang',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Unit%20Audit%20Dalam&pagetitle=Unit%20Audit%20Dalam',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Unit%20Integriti&pagetitle=Unit%20Integriti',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Pengurusan%20Sumber%20Manusia&pagetitle=Bahagian%20Pengurusan%20Sumber%20Manusia',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Bahagian%20Pentadbiran&pagetitle=Bahagian%20Pentadbiran',
        'https://www.mot.gov.my/my/directory/staff?bahagian=Pusat%20Logistik%20Negara&pagetitle=Pusat%20Logistik%20Negara'
    ]

    person_sort_order = 0  #init

    def parse(self, response):
        #index of the current URL in start_urls to define the division_sort_order
        current_url = response.url
        division_sort_order = self.start_urls.index(current_url) + 1  #add 1 to start from 1

        # Select the 2nd container (the first is assumed to be empty)
        container = response.xpath("(//div[@class='grid-container staff-directory'])[2]")

        # Extract the rows from the table
        rows = container.xpath(".//table[@class='stdirectory unstriped stack']/tbody/tr")

        for row in rows:
            self.person_sort_order += 1

            person_name = row.xpath(".//td[@data-label='Nama']/span[1]/text()").get(default='').strip()
            person_position = row.xpath(".//td[@data-label='Nama']/span[2]/text()").get(default='').strip()

            #collect all divisions into a list, accounting for multiple <span> and <br /> tags
            division_elements = row.xpath(".//td[@data-label='Division']//span/text() | .//td[@data-label='Division']//br/following-sibling::text()").getall()
            divisions = [division.strip() for division in division_elements if division.strip()]  #clean and collect divisions

            #assign the first element as division, second as unit (if available)
            division = divisions[0] if len(divisions) > 0 else None
            unit = divisions[1] if len(divisions) > 1 else None

            person_email_prefix = row.xpath(".//td[@data-label='Email']/span/text()").get(default='').strip()
            person_phone = row.xpath(".//td[@data-label='Telefon']/span/text()").get(default='').strip()
            person_email = f"{person_email_prefix}@mot.gov.my" if person_email_prefix else None

            yield {
                'agency': "KEMENTERIAN PENGANGKUTAN",
                'division_sort_order': division_sort_order,  #based on "start_urls" sequence
                'person_sort_order': self.person_sort_order,  #global
                'person_name': person_name,
                'division': division,  #first element of the division list
                'unit': unit,  #second element of the division list (if exists)
                'person_position': person_position,
                'person_phone': person_phone,
                'person_email': person_email,
            }
