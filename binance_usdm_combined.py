import requests
import pandas as pd
import numpy as np
import time
import requests
import matplotlib.pyplot as plt
import intro
from binance_usdm_asset import listing_df
from binance_usdm_ohlcv import df as datadf
from binance_usdm_funding import fundingdf

comdf = datadf.copy()
comdf['funding'] = None
updates = {}

for asset in comdf['asset'].unique():
    
    tempdatadf = datadf[datadf['asset'] == asset].reset_index(drop = False)
    tempfundingdf = fundingdf[fundingdf['asset'] == asset].reset_index(drop = False)
    
    for index in tempdatadf.index[1:]:
        
        oldtime = tempdatadf['timestamp'].iloc[index-1]
        newtime = tempdatadf['timestamp'].iloc[index]
        
        fundingperiod = tempfundingdf[(tempfundingdf['timestamp'] > oldtime) & (tempfundingdf['timestamp'] <= newtime)]
        _fundingperiod = fundingperiod['funding'].astype(float)
        funding = sum(_fundingperiod)
        
        updates[tempdatadf['index'].iloc[index-1]] = funding
        
comdf.loc[list(updates.keys()), 'funding'] = list(updates.values())
        
print('----------- saving binance_ohlcvf_data.csv -----------')
comdf.to_csv("binance_ohlcvf_data.csv", index=False)
        
        
        
        
        
        
        