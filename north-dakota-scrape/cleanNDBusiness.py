from pandas import DataFrame

class cleanNDBusiness:
  def clean_company_data(self, refined_company_list:list):
    for top_lvl_record in refined_company_list:
      temp_owner_list = []
      top_lvl_record['company_name_upper'] = top_lvl_record['company_name'].upper()
      for second_lvl_record in top_lvl_record['data']['DRAWER_DETAIL_LIST']:
        if second_lvl_record['LABEL'] in ['Owner Name', 'Registered Agent', 'Commercial Registered Agent', 'Owners', ""]:
          temp_owner_list.append({
            "owner_name": second_lvl_record['VALUE'].split('\n')[0].replace("  ", " "),
            "owner_name_upper": second_lvl_record['VALUE'].split('\n')[0].replace("  ", " ").upper(),
            "owner_type": second_lvl_record['LABEL']
            })
          top_lvl_record['owner'] = temp_owner_list
    return refined_company_list

  def generate_owner_dataframe(self, cleaned_company_data:list):
    owner_list = []
    for record in cleaned_company_data:
      for sub_record in record['owner']:
        sub_record['company_id'] = record['id']
        sub_record['company_name'] = record['company_name']
        sub_record['company_name_upper'] = record['company_name_upper']
        owner_list.append(sub_record)
    owner_df = DataFrame(owner_list)
    return owner_df