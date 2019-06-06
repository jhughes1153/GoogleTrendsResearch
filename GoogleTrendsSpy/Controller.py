'''
Created on Oct 27, 2018

A module that will deal with controlling what goes in and out of the database

@author: Jack
'''
import sys

import datetime as dt
from ScrapeGoogleTrends import GoogleTrends
from ScrapePricingDataYahoo import DataDiviScrape
from CommWithDatabase import HandleDB
from scrap_iex_data import grab_iex_df
import data_collector


def check_day():
    if dt.date.today().weekday() in range(1, 6):
        return True
    else:
        return False


def main_2(db):
    """
    Handle the movement of the data into the database
    :param db:
    :return:
    """
    print("appending to database")
    keywords_list = ["Trump", "President", "Debt", "Loan", "Mortgage", "Utilities", "Dow Jones", "Stock market",
                     "Trading", "Dog",
                     "Spy", "S&P", "Economy", "Election", "Apple", "Politics", "Unemployment", "Interest rates",
                     "Fed funds rate"]

    df_goog = data_collector.google_trends(keywords_list)
    print(df_goog.head())

    # save to csv just in case
    today = dt.date.today()
    df_goog.to_csv('/home/jack/logging/google_trends_{}.csv'.format(str(today)))

    df_yahoo_price, df_yahoo_divi = data_collector.yahoo_pricing("SPY")

    # Why is this having so many issues?
    db.append_to_database(df_yahoo_price.iloc[[-1]], "SPYPRICING_YAHOO")
    db.append_to_database(df_yahoo_divi, "SPYDIVI_YAHOO")
    db.append_to_database(df_goog, "SPYKEYWORDS")
    if check_day():
        db.append_to_database(data_collector.iex_pricing("spy"), "SPYPRICING_IEX")
        db.append_to_database(data_collector.iex_pricing("aapl"), "AAPLPRICING_IEX")
    else:
        print('Not doing iex today as it is a t+1 table')

    print("Finished appending tables")

    with open('/home/jack/cronwrapper.txt', 'w') as writer_file:
        writer_file.write('Finished writting')

    
def main(db):
    """A way to append to the database, it gets the most recent date
    :param db: HandleDB object
    """
    print("appending to database")
    keywords_list = ["Trump", "President", "Debt", "Loan", "Mortgage", "Utilities", "Dow Jones", "Stock market", "Trading", "Dog",
                     "Spy", "S&P", "Economy", "Election", "Apple", "Politics", "Unemployment", "Interest rates", "Fed funds rate"]
  
    GoogT = GoogleTrends("SPY", keywords_list)
      
    df_goog = GoogT.return_dataframe()
    print(df_goog.head())
      
    # save to csv just in case
    today = dt.date.today()
    df_goog.to_csv('/home/jack/logging/google_trends_{}.csv'.format(str(today)))
    
    ScrapeYahoo = DataDiviScrape("SPY")

    df_yahoo_price, df_yahoo_divi = ScrapeYahoo.return_dataframes()
    
    # Why is this having so many issues?
    db.append_to_database(df_yahoo_price.iloc[[-1]], "SPYPRICING_YAHOO")
    db.append_to_database(df_yahoo_divi, "SPYDIVI_YAHOO")
    db.append_to_database(df_goog, "SPYKEYWORDS")
    if check_day():
        db.append_to_database(grab_iex_df(), "SPYPRICING_IEX")
    else:
        print('Not doing iex today as it is a t+1 table')
    
    print("Finished appending tables")
    
    with open('/home/jack/cronwrapper.txt', 'w') as writer_file:
        writer_file.write('Finished writting')
    

if __name__ == '__main__':
    database = HandleDB()
    sys.exit(main_2(database))

