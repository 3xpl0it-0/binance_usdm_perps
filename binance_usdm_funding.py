import requests
import pandas as pd
import numpy as np
import time
import requests
import matplotlib.pyplot as plt
from binance_usdm_asset import listing_df

if __name__ == "__main__":
    import info

BINANCE_BASE_URL = 'https://fapi.binance.com'
BINANCE_FUNDING_URL = '/fapi/v1/fundingRate'
BINANCE_FUNDINGINFO_URL = '/fapi/v1/fundingInfo'

def get_funding(
    listing_data: list | None = None,
    base_url: str | None = None,
    funding_url: str | None = None
) -> list[tuple[int, str, float]]:
    
    if not listing_data:
        _listing_data = ASSET_LISTING
    else:
        _listing_data = listing_data
        
    if not base_url:
        _base_url = BINANCE_BASE_URL
    else:
        _base_url = base_url
        
    if not funding_url:
        _funding_url = BINANCE_FUNDING_URL
    else:
        _funding_url = funding_url
        
    funding = []
    url = f'{_base_url}{_funding_url}'
    
    for asset in _listing_data:
        
        start = int(asset[1])
        end = int(asset[2])
    
        try:
            while start < end:
                
                params = {
                    "symbol": asset[0],
                    "startTime": start,
                    "limit": 900
                }
                
                response = requests.get(url, params=params)    
                data = response.json()
                        
                for i in data[:-1]:
                    if i["fundingTime"] and i["symbol"] and i["fundingRate"]:
                        funding.append((i["fundingTime"], i["symbol"], i["fundingRate"])) # open time, open, high, low, close, volume
                    else:
                        print(f'problem {asset[0]} start {start}')
                        
                start = int(data[-1]['fundingTime'])
                time.sleep(.5)
                
        
        except requests.exceptions.RequestException as e:
            print(f"Warning: API request failed for {asset[0]}: {e}")
            print('because of this returning blank list')
        
            return []
        
        print(f'{asset} funding saved')
        
    return funding
    
def get_fundinginterval(
    base_url: str | None = None,
    fundinginfo_url: str | None = None
) -> list[tuple[str, int]]:
    
    if not base_url:
        _base_url = BINANCE_BASE_URL
    else:
        _base_url = base_url
        
    if not fundinginfo_url:
        _fundinginfo_url = BINANCE_FUNDINGINFO_URL
    else:
        _fundinginfo_url = fundinginfo_url
        
    fundinginterval = []
    
    url = f'{_base_url}{_fundinginfo_url}'
    
    try:
        response = requests.get(url)    
        data = response.json()
                    
        for i in data:
            fundinginterval.append((i["symbol"], i["fundingIntervalHours"])) # open time, open, high, low, close, volume
        
        return fundinginterval
        
    except requests.exceptions.RequestException as e:
        print(f"Warning: API request failed: {e}")
        print('because of this returning blank list')
        
        return []
    
ASSET_LISTING = listing_df.values.tolist()
    
print('----------- getting funding data -----------')
print('')

funding_data = get_funding()

print('----------- finished getting funding data -----------')
print('')
    
columns = ['timestamp', 'asset', 'funding']
fundingdf = pd.DataFrame(funding_data, columns= columns)

fundingintervals = get_fundinginterval()

columns = ['asset', 'interval']
fintervaldf = pd.DataFrame(fundingintervals, columns = columns)
fundingproblems = []

print('----------- checking funding data for missing entries -----------')
print('')

for asset in listing_df['asset']:
    
    tempdf = fundingdf[fundingdf['asset'] == asset]
    tempdf = tempdf.reset_index(drop=True)
    interval = fintervaldf[fintervaldf['asset']==asset]['interval'].squeeze()
    
    for index in tempdf.index[1:]:
        
        oldtime = tempdf['timestamp'].iloc[index-1]
        newtime = tempdf['timestamp'].iloc[index]
                
        if (newtime - oldtime < 3600000*interval - 3600000*.1) or (newtime - oldtime > 3600000*interval + 3600000*.1):
            
            fundingproblems.append([asset, newtime, oldtime, newtime-oldtime, interval])
        
        oldtime = newtime

columns = ['asset', 'old_time', 'new_time', 'difference', 'interval']
fundingprobdf = pd.DataFrame(fundingproblems, columns = columns)

# funding interval updates but have no info on what previous interval was
# to gain an estimate on whether its just a change in interval or if theres missing entries:
# funding can be 3600000 (1hour), 14400000 (4hours) or 28800000 (8hours)
# if the difference is wrong respective to current interval but is consistently one of the above then can estimate its not missing data

# first checking everything that has more than one difference

for asset in fundingprobdf['asset'].unique():
    
    tempdf = fundingprobdf[fundingprobdf['asset'] == asset]
    
    if max(tempdf["difference"]) - min(tempdf["difference"]) > 3600000*.1:
        
        print(f'asset: {asset} min: {min(tempdf["difference"])} mean: {tempdf["difference"].mean()} max: {max(tempdf["difference"])} count: {len(tempdf["difference"])}')
        
        bucket1 = min(tempdf["difference"]) + 3600000*.1
        tempdf_ = tempdf[tempdf['difference'] < bucket1]
        if max(tempdf_["difference"]) - min(tempdf_["difference"]) > 3600000*.1 or len(tempdf_["difference"]) < 5:
            print('--- bucket 1 ---')
            print(f'asset: {asset} min: {min(tempdf_["difference"])} mean: {tempdf_["difference"].mean()} max: {max(tempdf_["difference"])} count: {len(tempdf_["difference"])}')
        
        
        bucket2 = max(tempdf["difference"]) - 3600000*.1
        tempdf_ = tempdf[tempdf['difference'] > bucket2]
        if max(tempdf_["difference"]) - min(tempdf_["difference"]) > 3600000*.1 or len(tempdf_["difference"]) < 5:
            print('--- bucket 2 ---')
            print(f'asset: {asset} min: {min(tempdf_["difference"])} mean: {tempdf_["difference"].mean()} max: {max(tempdf_["difference"])} count: {len(tempdf_["difference"])}')

# checking differences where count is low (could be higher chance of missing data here)

for asset in fundingprobdf['asset'].unique():
    
    tempdf = fundingprobdf[fundingprobdf['asset'] == asset]
    
    if len(tempdf["difference"]) < 5:
        
        print(f'asset: {asset} min: {min(tempdf["difference"])} mean: {tempdf["difference"].mean()} max: {max(tempdf["difference"])} count: {len(tempdf["difference"])}')
    
print('----------- finished checking funding data for missing entries -----------')
print('')

if __name__ == "__main__":
    
    print('----------- saving binance_funding_data.csv -----------')
    fundingdf.to_csv("binance_funding_data.csv", index=False)

