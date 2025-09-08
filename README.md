# data collection for binance usdm perp contracts
collects all actively traded contracts that end in USDT

## binance_usdm_asset
gets symbol names, beginning date and most recent date

## binance_usdm_ohlcv
gets daily timestamp, open, high, low, close and volume for each symbol

## binance_usdm_funding
gets hourly funding for each symbol

## binance_usdm_combined
combines ohlcv and funding into daily data

## notes
running binance_usdm_combined will run all previous as it uses them  
running each script seperately will give you a csv file of the data that script collects  
the api can be dodgy sometimes so to combat this error messages will print and the code will stop if this happens (to make you aware) - easy fix is just to rerun it  
if there's problems or anything that seems like it should be improved reach out to me  
i've uploaded a csv file too if any of you just simply want the data  
