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
BINANCE_KLINE_URL = '/fapi/v1/klines'

def get_kline(
    listing_data: list | None = None,
    base_url: str | None = None,
    kline_url: str | None = None
) -> list[tuple[str, int, int, int, int, int, int, int]]:
    
    if not listing_data:
        _listing_data = ASSET_LISTING
    else:
        _listing_data = listing_data
    
    if not base_url:
        _base_url = BINANCE_BASE_URL
    else:
        _base_url = base_url
        
    if not kline_url:
        _kline_url = BINANCE_KLINE_URL
    else:
        _kline_url = kline_url
    
    kline = []
    url = f'{_base_url}{_kline_url}'
    
    for asset in _listing_data:
        start = asset[1]
        end = asset[2]
        
        try:
            while start < end:
                
                params = {
                    "symbol": asset[0],
                    "startTime": start,
                    "limit": 1000,
                    "interval": "1d"
                }
                
                response = requests.get(url, params=params)    
                data = response.json()
                
                for i in data[:-1]:
                    kline.append((asset[0], i[0], i[1], i[2], i[3], i[4], i[7])) # open time, open, high, low, close, volume
                    
                start = data[-1][0]
                
                time.sleep(.5)
                    
        except requests.exceptions.RequestException as e:
            print(f"Warning: API request failed for {asset[0]}: {e}")
            print('because of this returning blank list')
            
            return []
    
        print(f'{asset} kline saved')
    
    return kline

ASSET_LISTING = listing_df.values.tolist()

print('----------- getting kline data -----------')
print('')

asset_data = get_kline()

print('')
print('----------- finished getting kline data -----------')
print('')

columns = ["asset", "timestamp", "open", "high", "low", "close", "volume"]
df = pd.DataFrame(asset_data, columns=columns)

print('----------- checking for missing entries -----------')
print('')

for index in df.index[1:]:
    
    if df['timestamp'].iloc[index] - df['timestamp'].iloc[index-1] != 86400000 and df['asset'].iloc[index] == df['asset'].iloc[index-1]:
        print(f'problem: {df["timestamp"].iloc[index] - df["timestamp"].iloc[index-1]}')
        
print('----------- finished checking for missing entries -----------')
print('')

print('----------- checking for discrepencies in open - close -----------')
print('')
for index in df.index[1:]:
    
    open_ = float(df['open'].iloc[index])
    prev_close = float(df['close'].iloc[index-1])
    
    if open_ - prev_close > open_*0.005 and df['asset'].iloc[index] == df['asset'].iloc[index-1]:
        print(f'problem: (time: {df["timestamp"].iloc[index]} - {(open_ - prev_close)/prev_close}')
        print(f'asset: {df["asset"].iloc[index]} open: {open_} prev close: {prev_close}')

print('----------- finished checking for discrepencies in open - close -----------')
print('')

print('----------- checking for 0/Nan values -----------')
print('')

print(df.isna().any())

print('----------- finished checking for 0/Nan values -----------')
print('')

if __name__ == "__main__":

    print('----------- saving binance_data.csv -----------')
    df.to_csv("binance_data.csv", index=False)
