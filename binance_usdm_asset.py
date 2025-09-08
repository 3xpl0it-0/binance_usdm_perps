import requests
import pandas as pd
import numpy as np
import time
import requests
import matplotlib.pyplot as plt

if __name__ == "__main__":
    import info

BINANCE_BASE_URL = 'https://fapi.binance.com'
BINANCE_SYMBOL_URL = '/fapi/v1/exchangeInfo'
BINANCE_KLINE_URL = '/fapi/v1/klines'

def get_all_markets(
    base_url: str | None = None,
    symbol_url: str | None = None
) -> set[str]:
    
    if not base_url:
        _base_url = BINANCE_BASE_URL
    else:
        _base_url = base_url
        
    if not symbol_url:
        _symbol_url = BINANCE_SYMBOL_URL
    else:
        _symbol_url = symbol_url

    url = f"{_base_url}{_symbol_url}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
    
        assets = [symbol['symbol'] for symbol in data["symbols"] if symbol['status'] == 'TRADING']
        sorting = {asset for asset in assets if asset.endswith("USDT")}
        
        return sorting
    
    except requests.exceptions.RequestException as e:
        print(f"Warning: API request failed: {e}")
        print('because of this returning blank set')
        
        return set()

def get_listing_and_final_date(
    base_url: str | None = None,
    kline_url: str | None = None
) -> list[tuple[str, int, int]]:
    
    if not base_url:
        _base_url = BINANCE_BASE_URL
    else:
        _base_url = base_url
        
    if not kline_url:
        _kline_url = BINANCE_KLINE_URL
    else:
        _kline_url = kline_url
    
    symbols = get_all_markets()
    asset_listing = []
    
    url = f'{_base_url}{_kline_url}'
    
    for symbol in symbols:
        
        try:
    
            params1 = {
                "symbol": symbol,
                "interval": "1d",
                "startTime": 0,
                "limit": 1
            }
            
            params2 = {
                "symbol": symbol,
                "interval": "1d",
                "endTime": int(time.time() * 1000),
                "limit": 1
            }
    
            response1 = requests.get(url, params=params1)
            data1 = response1.json() # listing date
            
            response2 = requests.get(url, params=params2)
            data2 = response2.json() # final date
            
            asset_listing.append((symbol, data1[0][0], data2[0][0]))
            print(f'{symbol} ticker - start - end saved')
            
        except requests.exceptions.RequestException as e:
            print(f"Warning: API request failed: {e}")
            print('because of this returning blank list')
            
            return []
    
    return asset_listing

print('----------- getting symbols & start + end dates -----------')
print('')

_asset_listing = get_listing_and_final_date()

columns = ["asset", "listingdate", "currentdate"]
listing_df = pd.DataFrame(_asset_listing, columns= columns)

print('')
print('----------- finished getting symbols & start + end dates -----------')
print('')

if __name__ == "__main__":

    print('----------- saving binance_asset_listingdata.csv -----------')
    listing_df.to_csv("binance_asset_listingdata.csv", index=False)
