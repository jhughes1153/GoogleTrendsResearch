import requests
import pandas as pd
from argparse import ArgumentParser
import datetime as dt
from CommWithDatabase import HandleDB


def grab_iex_df(stock: str, date: str) -> pd.DataFrame:
    iex_values = requests.get(f"https://api.iextrading.com/1.0/stock/{stock}/chart/date/{date}")

    print(iex_values.url)

    iex_df = pd.DataFrame(iex_values.json())
    iex_df.columns = [c.upper() for c in iex_df.columns]
    iex_df = iex_df.rename(index=str, columns={'DATE': 'TRADEDATE'})
    iex_df['RECEIVETIME'] = pd.to_datetime(iex_df['TRADEDATE'] + iex_df["MINUTE"], format="%Y%m%d%H:%M")
    iex_df = iex_df[["TRADEDATE", "RECEIVETIME", "HIGH", "LOW", "AVERAGE", "VOLUME", "NOTIONAL", "NUMBEROFTRADES",
                     "MARKETHIGH", "MARKETLOW", "MARKETAVERAGE", "MARKETVOLUME", "MARKETNUMBEROFTRADES", "OPEN",
                     "CLOSE", "MARKETOPEN", "MARKETCLOSE", "CHANGEOVERTIME", "MARKETCHANGEOVERTIME"]]
    print(iex_df.shape)

    return iex_df


def dates_between_no_weekends(start_date):
    dates = []
    today = dt.date.today()
    while start_date != today:
        print(start_date, start_date.weekday())
        if start_date.weekday() not in (5, 6):
            dates.append(start_date.strftime('%Y%m%d'))
        start_date += dt.timedelta(days=1)

    return dates


def main():
    parser = ArgumentParser(description="Regenerate data for up to a month ago")
    parser.add_argument('-s', '--start-date', help="date to start with in YYYMMDD format")
    parser.add_argument('-S', '--stock', help="stock ticker to use")
    args = parser.parse_args()
    print(args)

    db = HandleDB()

    starter = dt.datetime.strptime(args.start_date, '%Y%m%d').date()
    dates = dates_between_no_weekends(starter)
    print(dates)

    grab_iex_df(f"{args.stock}", '20190617'), f"{args.stock}PRICING_IEX"

    # for d in dates:
    #     print(d)
    #     try:
    #         db.append_to_database(grab_iex_df(f"{args.stock}", d), f"{args.stock}PRICING_IEX")
    #     except KeyError:
    #         print(f'Failed for {d}')

    return 0


if __name__ == '__main__':
    main()
