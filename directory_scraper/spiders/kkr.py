import scrapy
import re

class KKRSpider(scrapy.Spider):
    name = 'kkr'
    start_urls = ['https://www.kkr.gov.my/ms/direktori?field_tax_bahagian_target_id=All&title=']

    def parse(self, response):
        tables = response.xpath('//table')

        for table in tables:
            division = table.xpath('preceding-sibling::div[contains(@class, "field--name-field-bahagian")][1]/div[@class="field__item"]/text()').get().strip()

            unit = table.xpath('caption/text()').get().strip()
            unit = unit.split('|')[0].strip()

            normalized_division = self.normalize_string(division)
            normalized_unit = self.normalize_string(unit)

            if self.strings_are_similar(normalized_division, normalized_unit):
                unit = None #if division=unit, then unit = None.

            rows = table.xpath('.//tbody/tr')
            for row in rows:
                person_name = row.xpath('.//td[1]/text()').get().strip()
                person_position = row.xpath('.//td[2]/text()').get().strip()
                person_email = row.xpath('.//td[3]/text()').get().strip()
                person_phone = row.xpath('.//td[4]/text()').get().strip()

                yield {
                    'agency': 'KEMENTERIAN KERJA RAYA',
                    'person_name': person_name,
                    'division': division,
                    'unit': unit,
                    'person_position': person_position,
                    'person_email': person_email,
                    'person_phone': person_phone,
                }

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