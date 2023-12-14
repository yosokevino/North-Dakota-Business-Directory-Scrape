from  scrapeNDBusiness import scrapeNDBusiness
from cleanNDBusiness import cleanNDBusiness
from asyncio import get_event_loop

if __name__ == '__main__':
    search_dict = {
    "ACTIVE_ONLY_YN": "true",
    "SEARCH_VALUE": "X",
    "STARTS_WITH_YN": "true"
    } 
    print(f"Starting Scrape with the following params {search_dict}...")
    nd_scrape = scrapeNDBusiness()
    company_search_results = nd_scrape.business_search(search_dict)
    print(f"{len(company_search_results)} companies found...")
    refined_company_list = []
    loop = get_event_loop()
    data = loop.run_until_complete(nd_scrape.async_pull(company_search_results[:15]))
    refined_company_list += data
    print(f"{len(refined_company_list)} company records saved...")
    nd_clean = cleanNDBusiness()
    cleaned_company_data = nd_clean.clean_company_data(refined_company_list)
    owner_dataframe = nd_clean.generate_owner_dataframe(cleaned_company_data)
    print(owner_dataframe.to_markdown(tablefmt="grid"))
    print(f"Scrape complete")