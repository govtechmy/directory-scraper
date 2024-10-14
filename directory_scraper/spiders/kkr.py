import scrapy
import re

class KKRSpider(scrapy.Spider):
    name = 'kkr'
    start_urls = ['https://www.kkr.gov.my/ms/direktori?field_tax_bahagian_target_id=All&title=']
    
    person_counter = 0 #init
    division_counter = 0 #init

    last_processed_division = None

    def parse(self, response):
        tables = response.xpath('//table')

        for table in tables:
            division = table.xpath('preceding-sibling::div[contains(@class, "field--name-field-bahagian")][1]/div[@class="field__item"]/text()').get().strip()

            unit = table.xpath('caption/text()').get().strip()
            unit = unit.split('|')[0].strip()

            normalized_division = self.normalize_string(division)
            normalized_unit = self.normalize_string(unit)

            if self.strings_are_similar(normalized_division, normalized_unit):
                unit = None  #if division=unit, then unit = None.

            #check if this is a new division
            if division != self.last_processed_division:
                self.division_counter += 1  #increment division_sort only if it's a new division
                self.last_processed_division = division

            rows = table.xpath('.//tbody/tr')
            for row in rows:
                person_name = row.xpath('.//td[1]/text()').get().strip()
                person_position = row.xpath('.//td[2]/text()').get().strip()
                person_email = row.xpath('.//td[3]/text()').get().strip()
                person_phone = row.xpath('.//td[4]/text()').get().strip()

                yield {
                    'org_sort': 2,
                    'org_id': 'KKR',
                    'org_name': 'KEMENTERIAN KERJA RAYA',
                    'org_type': 'ministry',
                    'division_sort': self.division_counter,
                    'position_sort': self.person_counter,
                    'division_name': division if division else None,
                    'subdivision_name': unit if unit else None,
                    'person_name': person_name if person_name else None,
                    'position_name': person_position if person_position else None,
                    'person_phone': person_phone if person_phone else None,
                    'person_email': person_email if person_email else None,
                    'person_fax': None,
                    'parent_org_id': None,
                }

                self.person_counter += 1 #increment the person_sort_order after each person

    def normalize_string(self, text): #need to normalize before comparing whether division = unit. e.g division = "Bahagian Pembangunan & Penswastaan" -> "Bahagian Pembangunan dan Penswastaan
        text = text.lower()
        text = text.replace('&', 'dan')
        text = re.sub(r'[^\w\s]', '', text)  #remove special char
        text = re.sub(r'\s+', ' ', text)  #replace multiple spaces with a single space
        return text.strip()

    def strings_are_similar(self, str1, str2): 
        common_words = ['bahagian', 'unit', 'dan'] #remove common words that might appear in both division and unit
        for word in common_words:
            str1 = str1.replace(word, '')
            str2 = str2.replace(word, '')
        
        str1 = str1.strip() #remove leading/trailing whitespace
        str2 = str2.strip()
        
        return str1 == str2 #check if the remaining strings is same


#can use last_processed_division bcs the data is already sorted in table and row format