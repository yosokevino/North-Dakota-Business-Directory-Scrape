import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet import defer, reactor

class ndData(scrapy.Spider):
    name = "nd-data"
    def process_business_data(self, raw_json):
        refined_data = [
            {
                "id":record['ID'],
                "company_name":record['TITLE'][0], 
                "url": f"https://firststop.sos.nd.gov/api/FilingDetail/business/{record['ID']}/false"
            } for record in list(raw_json['rows'].values()) if record['TITLE'][0][0] == "x" or record['TITLE'][0][0] == "X"]

        return refined_data
    
    def start_requests(self):
        url = "https://firststop.sos.nd.gov/api/Records/businesssearch"
        search_dict = {
        "ACTIVE_ONLY_YN": "true",
        "SEARCH_VALUE": "X",
        "STARTS_WITH_YN": "true"}
        yield scrapy.http.JsonRequest(url=url, data=search_dict, callback=self.parse)

    def parse(self, response):
        data = response.json()
        self.output['data'] = self.process_business_data(data)

class companyData(scrapy.Spider):
    name = "company-data"

    def start_requests(self):
        refined_business_data = self.input
        for record in refined_business_data:
            url = record['url']
            print(url)
            yield scrapy.Request(url=url, meta={'record':record})

    def parse(self, response):
        record = response.meta['record']
        record['data'] = response.json()
        data = record
        data.update(data)
        yield data
        
custom_settings_1 = {
    'DEFAULT_REQUEST_HEADERS' : {
        "Content-Type":"application/json",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Authorization": "undefined"},
    'REQUEST_FINGERPRINTER_IMPLEMENTATION':"2.7"}
custom_settings_2 = {
    'DEFAULT_REQUEST_HEADERS' : {
        "Content-Type":"application/json",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Authorization": "undefined"},
    'REQUEST_FINGERPRINTER_IMPLEMENTATION':"2.7",
    'FEED_FORMAT': "json",
    'FEED_URI': "./response-data/scrapy_cleaned_company_data.json"}

process_1 = CrawlerProcess(custom_settings_1)
process_2 = CrawlerProcess(custom_settings_2)
@defer.inlineCallbacks
def crawl():
    all_business_data = {}
    all_business_data_detail = {}
    yield process_1.crawl(ndData, output = all_business_data)
    yield process_2.crawl(companyData, input = all_business_data['data'], output = all_business_data_detail)
    reactor.stop()
crawl()
reactor.run()

