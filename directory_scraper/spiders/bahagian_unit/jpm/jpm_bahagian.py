import scrapy

class JPM_DIVISION_Spider(scrapy.Spider):
    name = 'jpm_bahagian'
    start_urls = ['https://www.jpm.gov.my/ms/info-korporat/mengenai-kami#sppb-tab1-1']  
    def parse(self, response):
        bahagian_tabs = response.css('ul.sppb-nav-custom li a')
        bahagian_data = []

        for tab in bahagian_tabs:
            label = tab.css('::text').get().strip()
            href = tab.css('::attr(href)').get()
            content_id = href.lstrip('#')

            # Use the content ID to find the corresponding content, excluding <style> tags
            content_blocks = response.css(f'div.sppb-tab-pane#{content_id} .sppb-addon-content')

            content_texts = content_blocks.css(':not(style)::text').getall()
            cleaned_text = ' '.join(content_texts).strip()
            cleaned_text = ' '.join(cleaned_text.split())

            bahagian_data.append({
                'org_id': 'JPM',
                'division_name': label,
                'division_func': cleaned_text,
                # 'links': links  # Add links to the output
            })

        for item in bahagian_data:
            yield item
