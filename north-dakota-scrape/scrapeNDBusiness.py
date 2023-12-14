from requests import Session
from aiohttp import ClientSession
from asyncio import gather

class scrapeNDBusiness:
  def __init__(self):
    self.s = Session()
    self.s.headers = {
      "Content-Type":"application/json",
      "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
      "Authorization": "undefined"}

  def business_search(self, search_dict:dict, refine_results=True):
    base_url = "https://firststop.sos.nd.gov/api/Records/businesssearch"
    init_response = self.s.post(base_url, json=search_dict)
    raw_json = init_response.json()
    if refine_results == True:
      return [{"id":record['ID'], "company_name":record['TITLE'][0], "url": f"https://firststop.sos.nd.gov/api/FilingDetail/business/{record['ID']}/false"} for record in list(raw_json['rows'].values()) if record['TITLE'][0][0] == "x" or record['TITLE'][0][0] == "X"]
    else:
      return raw_json
  
  async def async_fetch(self, session, record):
      async with session.get(record['url']) as response:
        record['data'] = await response.json()
        return record

  async def async_pull(self, company_search_results:list):
    headers = {
        "Content-Type":"application/json",
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Authorization": "undefined"}
    async with ClientSession(headers=headers) as session:
      tasks = [self.async_fetch(session, record) for record in company_search_results]
      data = await gather(*tasks, return_exceptions=True)
      combined_result = []
      for temp_dict in data:
        combined_result += [temp_dict]
      return data