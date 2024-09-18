import re
import json
import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class MOTACSpider(CrawlSpider):
    name = "motac"
    allowed_domains = ["www.motac.gov.my"]
    start_urls = ["https://www.motac.gov.my/direktori"]

    rules = (
        Rule(LinkExtractor(allow="direktori"), callback='parse_item'),
    )

    def parse_item(self, response):
        division = response.css("h1::text").get()
        phone_regex = re.compile(r"03[\d\- ]+\d")

        directory_data = [
            [data for data in re.split(r"[\r\t\n]{2,}", re.sub(r"<.+?>", "", person_data))[1:-1] if data]
            for person_data in response.css("td[class='uk-width-7-10']").getall()
        ]

        for datapoint in directory_data:
            name = datapoint[0].strip()
            position = datapoint[1].strip()
            unit = datapoint[2].strip() if len(datapoint) > 3 else None
            email = email_str[0].strip() if (email_str := re.sub(pattern=phone_regex, repl="", string=datapoint[-1])) else None
            phone = phone_str[0].strip() if (phone_str := re.findall(pattern=phone_regex, string=datapoint[-1])) else None

            person_data = {
                "agency": "KEMENTERIAN PELANCONGAN, SENI DAN BUDAYA",
                "person_name": name,
                "person_email": email,
                "person_phone": phone,
                "person_position": position,
                "unit": unit,
                "division": division
            }

            yield person_data