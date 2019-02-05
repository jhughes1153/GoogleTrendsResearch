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
      
    # save to dataframe just in case
    today = dt.date.today()
    df_goog.to_csv('/home/jack/logging/google_trends_{}.csv'.format(str(today)))
    
    ScrapeYahoo = DataDiviScrape("SPY")

    df_yahoo_price, df_yahoo_divi = ScrapeYahoo.return_dataframes()
    
    # Why is this having so many issues?
    db.append_to_database(df_yahoo_price.iloc[[-1]], "SPYPRICING_YAHOO")
    db.append_to_database(df_yahoo_divi, "SPYDIVI_YAHOO")
    db.append_to_database(df_goog, "SPYKEYWORDS")
    
    print("Finished appending tables")
    
    with open('/home/jack/cronwrapper.txt', 'w') as writer_file:
        writer_file.write('Finished writting')
    

if __name__ == '__main__':
    db = HandleDB()
    sys.exit(main(db))

