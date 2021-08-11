def data_cleaner():
    import pandas as pd
    import re

    storedf=pd.read_csv('/usr/local/airflow/data/store_files/raw_store_transactions.csv',parse_dates=['Date'])

    storedf.drop("Date",axis="columns",inplace=True)

    storedf['Date']="2020-06-28"


    def clean_store_location(str_loc):
        return re.sub(r'[^\w\s]','',str_loc).strip()
        
    storedf['STORE_LOCATION'] =storedf['STORE_LOCATION'].map(lambda x:clean_store_location(x))


    def clean_product_id(pd_id):
        matches=re.findall(r'\d+',pd_id)
        if matches:
            return matches[0]
        return pd_id
        
    storedf['PRODUCT_ID']=storedf['PRODUCT_ID'].map(lambda x:clean_product_id(x))

    def remove_dollar_symbol(amount):
        return float(amount.replace("$",''));
        
    for to_clean in ['MRP','CP','SP','DISCOUNT']:
        storedf[to_clean]=storedf[to_clean].map(lambda x : remove_dollar_symbol(x))
        
    storedf.to_csv('/usr/local/airflow/data/store_files/clean_store_transactions.csv',index=False)

