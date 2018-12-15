'''
Created on Oct 27, 2018

A module that will deal with controlling what goes in and out of the database

@author: Jack
'''

from ScrapeGoogleTrends import GoogleTrends
from ScrapePricingDataYahoo import DataDiviScrape
from CommWithDatabase import HandleDB

def startup_db(db):
    """This is only run once basically to setup the db
    :param db: HandleDB object
    """
    print("starting up database")
    keywords_list = ["Trump", "President", "Debt", "Loan", "Mortgage", "Utilities", "Dow Jones", "Stock market", "Trading", "Dog", "Spy", "Economy", "Election", "Apple", "Politics", "Unemployment", "Interest rates", "Fed funds rate"]

    GoogT = GoogleTrends("SPY", keywords_list, False)
    
    df_goog = GoogT.return_dataframe()
    
    ScrapeYahoo = DataDiviScrape("SPY", False)

    df_yahoo_price, df_yahoo_divi = ScrapeYahoo.return_dataframes()

    db.append_to_database(df_yahoo_price, "spy_pricing")
    db.append_to_database(df_yahoo_divi, "spy_divis")
    db.append_to_database(df_goog, "spy_keywords")
    
    print("Finished creating tables")
    
def append_new_info(db):
    """A way to append to the database, it gets the most recent date
    :param db: HandleDB object
    """
    print("appending to database")
    keywords_list = ["Trump", "President", "Debt", "Loan", "Mortgage", "Utilities", "Dow Jones", "Stock market", "Trading", "Dog", "Spy", "Economy", "Election", "Apple", "Politics", "Unemployment", "Interest rates", "Fed funds rate"]

    GoogT = GoogleTrends("SPY", keywords_list)
    
    df_goog = GoogT.return_dataframe()
    
    ScrapeYahoo = DataDiviScrape("SPY")

    df_yahoo_price, df_yahoo_divi = ScrapeYahoo.return_dataframes()
    
    db.append_to_database(df_yahoo_price, "spy_pricing")
    db.append_to_database(df_yahoo_divi, "spy_divis")
    db.append_to_database(df_goog, "spy_keywords")
    
    print("Finished creating tables")
    

if __name__ == '__main__':
    db = HandleDB()
    create_or_append = db.check_if_tables_exist()
    if(create_or_append):
        append_new_info(db)
    else:
        startup_db(db)
    
    
    