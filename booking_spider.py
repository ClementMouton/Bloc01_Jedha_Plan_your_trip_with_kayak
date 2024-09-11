import os
import pandas as pd
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import random


cities_df = pd.read_csv('C:/Users/mouto/Documents/Jedha/dsfsft28/PROJECT/02_Kayak/files/top_5city.csv')

class Booking_Kayak(scrapy.Spider):

    name = "booking"
    start_urls = ['https://www.booking.com/']
    info_path = cities_df["name"].tolist()

    def parse(self, response):
        for city in self.info_path:
            self.city = city
            search_url = f"https://www.booking.com/searchresults.fr.html?ss={self.city}"
            yield scrapy.Request(
                url = search_url,
                callback = self.hotels_details,
                cb_kwargs= {"city" : city})

    def hotels_details(self, response, city):
        for info in response.xpath('//*[@data-testid = "property-card"]'):
#/html/body/div[4]/div/div[2]/div/div[2]/div[3]/div[2]/div[2]/div[3]/div[3]
            url = info.xpath('div[1]/div[1]/div/a').attrib["href"]
            data = {
                "city": city,
                "name": info.xpath('div[1]/div[2]/div/div/div[1]/div/div[1]/div/h3/a/div[1]/text()').get(),
                "note": info.xpath('div[1]/div[2]/div/div/div[2]/div/div[1]/a/span/div/div[1]/text()').get(),
                "url" : url,
                "description" : info.xpath('div[1]/div[2]/div/div/div[1]/div/div[3]/text()').get(),
}

            yield response.follow(url,callback = self.hotels_latlng, meta = data)

    def hotels_latlng(self, response):
        data = response.meta
        data['gps_hotel'] = response.xpath('//*[@id="hotel_address"]').attrib["data-atlas-latlng"]
        yield data

# Name of the file where the results will be saved
filename = "booking_spider.json"

# If file already exists, delete it before crawling (because Scrapy will 
# concatenate the last and new results otherwise)
if filename in os.listdir('files/'):
        os.remove('files/' + filename)


process = CrawlerProcess(settings = {
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'LOG_LEVEL': logging.INFO,
    # 'AUTOTHROTTLE_ENABLED' : True,
    # 'DOWNLOADER_MIDDLEWARES' : {
    #     'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    #     'scrapy_user_agents.middlewares.RandomUserAgentMiddleware': 400,
    # },
    "FEEDS": {
        'files/' + filename : {"format": "json"}
    }
})

# Start the crawling using the spider you defined above
process.crawl(Booking_Kayak)
process.start()