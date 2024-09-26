import scrapy

class NRESSpider(scrapy.Spider):
    name = 'nres'

    start_urls = ['https://www.nres.gov.my/_layouts/ketsaportal/VbForm/Direktori/DirektoriBio.aspx']

    def parse(self, response):
        #extract this hidden fields required for payload form submission
        viewstate = response.xpath('//input[@name="__VIEWSTATE"]/@value').get()
        viewstategenerator = response.xpath('//input[@name="__VIEWSTATEGENERATOR"]/@value').get()
        eventvalidation = response.xpath('//input[@name="__EVENTVALIDATION"]/@value').get()

        if not viewstate or not viewstategenerator or not eventvalidation:
            self.log("One or more required form fields not found.", level=scrapy.log.ERROR)
            return
        
        form_data = {
            '__EVENTTARGET': 'ddlBahagian',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstategenerator,
            '__EVENTVALIDATION': eventvalidation,
            'ddlBahagian': 'PEJABAT MENTERI',
            'tbAuthor': '',
        }

        yield scrapy.FormRequest(
            url='https://www.nres.gov.my/_layouts/ketsaportal/VbForm/Direktori/DirektoriBio.aspx',
            formdata=form_data,
            callback=self.parse_results
        )

    def parse_results(self, response):
        rows = response.xpath('//table[@id="gvSearchResults"]/tr[position() > 1]')  #skip header row

        if not rows:
            self.log("No rows found in the table. Check if the form submission was correct.", level=scrapy.log.ERROR)
            return

        for row in rows:
            name = row.xpath('td[1]/text()').get()
            bahagian = row.xpath('td[2]/text()').get()
            jawatan = row.xpath('td[3]/text()').get()

            yield {
                'name': name,
                'bahagian': bahagian,
                'jawatan': jawatan
            }
