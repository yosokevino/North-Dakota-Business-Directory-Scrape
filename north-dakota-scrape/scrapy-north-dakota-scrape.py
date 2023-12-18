import scrapy
from scrapy.crawler import CrawlerProcess
from twisted.internet import defer, reactor

class ndData(scrapy.Spider):
    name = "nd-data"
    def process_business_data(self, raw_json):
        """Processes the raw json returned into a cleaned list for futher processing"""
        refined_data = [
            {
                "id":record['ID'],
                "company_name":record['TITLE'][0], 
                "url": f"https://firststop.sos.nd.gov/api/FilingDetail/business/{record['ID']}/false"
            } for record in list(raw_json['rows'].values()) if record['TITLE'][0][0] == "x" or record['TITLE'][0][0] == "X"]

        return refined_data
    
    def start_requests(self):
        """Runs the first level scrape to retrieve company ids"""
        url = "https://firststop.sos.nd.gov/api/Records/businesssearch"
        search_dict = {
        "ACTIVE_ONLY_YN": "true",
        "SEARCH_VALUE": "X",
        "STARTS_WITH_YN": "true"}
        yield scrapy.http.JsonRequest(url=url, data=search_dict, callback=self.parse)

    def parse(self, response):
        """Scrapy parser that saves to an output variable"""
        data = response.json()
        self.output['data'] = self.process_business_data(data)

class companyData(scrapy.Spider):
    name = "company-data"

    def start_requests(self):
        """Pulls business details based in id from the input data"""
        refined_business_data = self.input
        for record in refined_business_data:
            url = record['url']
            print(url)
            yield scrapy.Request(url=url, meta={'record':record})

    def parse(self, response):
        """Retrieve and append raw json output"""
        record = response.meta['record']
        record['data'] = response.json()
        data = record
        data.update(data)
        yield data

# Defines custom settings for crawler processes
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

# Defines the crawler processes
process_1 = CrawlerProcess(custom_settings_1)
process_2 = CrawlerProcess(custom_settings_2)

# Uses inlineCallbacks to make sure the first level company data is pulled before the detail pull is started. Uses yield to ensure the sequence is followed.
@defer.inlineCallbacks
def crawl():
    """Crawler function that populates predefined dictionaries with crawl output"""
    all_business_data = {}
    # Collects first level business data from output variable
    yield process_1.crawl(ndData, output = all_business_data)
    # Passes previously collected data to new crawl and saves the raw data to a json file
    yield process_2.crawl(companyData, input = all_business_data['data'])
    reactor.stop()

# Calls the crawl function and starts the reactor
crawl()
reactor.run()

