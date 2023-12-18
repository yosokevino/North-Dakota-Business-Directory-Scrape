from  scrapeNDBusiness import scrapeNDBusiness
from cleanNDBusiness import cleanNDBusiness
from asyncio import get_event_loop
from datetime import datetime
import json
if __name__ == '__main__':
    then = datetime.now()
    # Defines a search dictionary to run
    search_dict = {
    "ACTIVE_ONLY_YN": "true",
    "SEARCH_VALUE": "X",
    "STARTS_WITH_YN": "true"
    } 
    print(f"Starting Scrape with the following params {search_dict}...")
    
    # Pulls initial company results list
    nd_scrape = scrapeNDBusiness()
    company_search_results = nd_scrape.business_search(search_dict)
    print(f"{len(company_search_results)} companies found...")

    # Pulls additional company details using async
    refined_company_list = []
    loop = get_event_loop()
    data = loop.run_until_complete(nd_scrape.async_pull(company_search_results))
    refined_company_list += data
    print(f"{len(refined_company_list)} company records saved...")

    # Cleans and saves the data for graph use later
    nd_clean = cleanNDBusiness()
    cleaned_company_data = nd_clean.clean_company_data(refined_company_list)
    with open("./response-data/cleaned_company_data.json", "w") as f:
        f.write(json.dumps(cleaned_company_data))
    owner_dataframe = nd_clean.generate_owner_dataframe(cleaned_company_data)
    owner_dataframe.to_csv("./response-data/owner_data2.csv", index = False)
    print(f"Scrape complete")

    now = datetime.now()
    time_diff = now-then
    print(time_diff)