import os
import re

import requests
import scrapy


def warn_on_generator_with_return_value_stub(spider, callable):
    pass


scrapy.utils.misc.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub
scrapy.core.scraper.warn_on_generator_with_return_value = warn_on_generator_with_return_value_stub

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}

headers_map = {
    'Art der Versteigerung:': 'Type of Auction',
    'Grundbuch:': 'Land Register',
    'Objekt/Lage:': 'Object/Location',
    'Beschreibung:': 'Description',
    'Verkehrswert in \x80:': 'Market Value in €',
    'Termin:': 'Auction Date & Time',
    'Ort der Versteigerung:': 'Auction Location',
    'Informationen zum Gläubiger:': 'Information About The Creditor',
    # Web Links
    'GeoServer:': 'Geo Server',
    'Exposee:': 'Exposure',
    'Gericht:': 'Court',
    # PDF's
    'amtliche Bekanntmachung': 'Official Announcement',
    'Foto:': 'Photo',
    'Gutachten:': 'Appraisal/Report',
}


class ZvgPortalSpider(scrapy.Spider):
    name = "zvg"
    start_urls = ["https://www.zvg-portal.de/index.php?button=Termine%20suchen"]
    base_url = "https://www.zvg-portal.de/index.php"
    states = None
    downloaded_folder = 'data'

    def __init__(self, states=None, download=None, *args, **kwargs):
        super(ZvgPortalSpider, self).__init__(*args, **kwargs)
        self.states = list(states)
        self.logger.info(f'Scraping states:{self.states}')
        self.download = download.lower()
        if self.download == 'yes':
            if not os.path.exists(self.downloaded_folder):
                # If folder doesn't exist, create it
                self.logger.info(f'Folder {self.downloaded_folder} is created')
                os.mkdir(self.downloaded_folder)

    def parse(self, response, **kwargs):
        for x in list(self.states):
            self.logger.info(f'Current scraping state:{x}')
            formdata = {
                "ger_name": "-- All local courts --",
                "order_by": "2",
                "land_abk": x,
                "ger_id": "0",
                "az1": "",
                "az2": "",
                "az3": "",
                "az4": "",
                "art": "",
                "obj": "",
                "str": "",
                "hnr": "",
                "plz": "",
                "ort": "",
                "ortsteil": "",
                "vtermin": "",
                "btermin": ""
            }
            url = 'https://www.zvg-portal.de/index.php?button=Suchen&all=1'
            yield scrapy.FormRequest(url, formdata=formdata, headers=headers, callback=self.parse_detail)

    def parse_detail(self, response):
        detail_urls = response.xpath("//tr[td[nobr[contains(text(), 'Aktenzeichen')]]]//a/@href").getall()

        for x in detail_urls:
            yield response.follow(x, headers=headers, callback=self.parse_detail_data)

    def clean_text(self, text):
        cleaned_text = re.sub(r'[\r\n]+', '', text)
        return cleaned_text

    def parse_detail_data(self, response):
        rows = response.xpath('//table[@id="anzeige"]//tr')

        dict = {"Status": "Active"}
        for i, row in enumerate(rows):
            try:
                if i == 0:
                    dict.update({
                        'File Number': row.xpath('.//td[1]//text()').get().strip().encode("ascii", "ignore").decode(),
                        'Last Updated On': re.sub(
                            r'(letzte Aktualisierung:|\(|\))', '', row.xpath('.//td[2]//text()').get()).strip()
                    })

                else:
                    key = headers_map[row.xpath('.//td[1]//text()').get().strip()]
                    value = self.clean_text(''.join(row.xpath('.//td[2]//text()').getall()).strip())
                    if key in ['Geo Server', 'Exposure', 'Official Announcement', 'Photo', 'Appraisal/Report', 'Court']:
                        value = row.xpath('.//td[2]/a/@href').get().strip()
                        name = row.xpath('.//td[2]//text()').get().strip()

                        temp = dict.get(key, [])

                        if key in ['Exposure', 'Official Announcement', 'Photo', 'Appraisal/Report']:
                            value = self.base_url + value

                        temp.append({'name': name, 'value': value})
                        value = temp
                    dict.update({
                        key: value
                    })
            except Exception:
                pass

        for x in headers_map.values():
            if x not in dict:
                dict.update({
                    x: None
                })

        dict.update({
            'url': response.url
        })
        yield dict

        if self.download == 'yes':
            self.logger.info(f'Downloading Documents related to {response.url}')
            for x in ['Exposure', 'Official Announcement', 'Photo', 'Appraisal/Report']:
                url_objects = dict[x]
                if url_objects:
                    for url_object in url_objects:
                        file_name = url_object["name"]
                        url = url_object["value"]
                        yield scrapy.Request(url, callback=self.save_pdf, headers=headers,
                                             cb_kwargs={'filing_number': dict['File Number'], 'file_name': file_name})

    def save_pdf(self, response, filing_number, file_name):
        # Extract product name or any other identifier from response if needed
        folder = filing_number.replace('/', '-')
        if not os.path.exists(f'{self.downloaded_folder}/{folder}'):
            # If folder doesn't exist, create it
            self.logger.debug(f'Folder {folder} is created')
            os.mkdir(f'{self.downloaded_folder}/{folder}')
        name = file_name.split('.')[0]
        # Save PDF file with product name as filename
        with open(f'{self.downloaded_folder}/{folder}/{name}.pdf', 'wb') as f:
            f.write(response.body)
            self.logger.debug(f'Downloaded {name}')
