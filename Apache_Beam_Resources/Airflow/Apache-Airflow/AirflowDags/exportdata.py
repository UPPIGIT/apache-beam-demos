
import pandas as pd

cleanStoredf =pd.read_csv('/usr/local/airflow/data/store_files/clean_store_transactions.csv',parse_dates=['Date'])
cleanStoredf['PROFIT'] =cleanStoredf['SP']-cleanStoredf['CP']

def exportStorewiseData():
    storewiseProfit=cleanStoredf.groupby(["STORE_ID","Date"]).agg({"PROFIT" : "sum"})
    storewiseProfit.to_csv("/usr/local/airflow/data/store_files/store_wise_profit.csv")
    
def locationwiseData():
    locationwiseProfit=cleanStoredf.groupby(["STORE_LOCATION","Date"]).agg({"PROFIT" : "sum"})
    locationwiseProfit.to_csv("/usr/local/airflow/data/store_files/location_wise_profit.csv")