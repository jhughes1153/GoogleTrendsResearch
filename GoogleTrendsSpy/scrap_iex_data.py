import requests
import pandas as pd


def grab_iex_df():
    iex_values = requests.get("https://api.iextrading.com/1.0/stock/aapl/chart/1d")

    iex_df = pd.DataFrame(iex_values.json())
    iex_df.columns = [c.upper() for c in iex_df.columns]
    iex_df = iex_df.rename(index=str, columns={'DATE': 'TRADEDATE'})
    iex_df['RECEIVETIME'] = pd.to_datetime(iex_df['TRADEDATE'] + iex_df["MINUTE"], format="%Y%m%d%H:%M")
    print(iex_df.dtypes)
    iex_df = iex_df[["TRADEDATE", "RECEIVETIME", "HIGH", "LOW", "AVERAGE", "VOLUME", "NOTIONAL", "NUMBEROFTRADES",
                     "MARKETHIGH", "MARKETLOW", "MARKETAVERAGE", "MARKETVOLUME", "MARKETNUMBEROFTRADES", "OPEN",
                     "CLOSE", "MARKETOPEN", "MARKETCLOSE", "CHANGEOVERTIME", "MARKETCHANGEOVERTIME"]]

    return iex_df
