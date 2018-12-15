'''
Created on Oct 27, 2018

This module will be about adding info to the database and basically just appending to it, this database will basically only have
information pertaining to the spy

@author: Jack
'''

from sqlalchemy import create_engine
import pandas as pd

class HandleDB:
    """class that will open a connection into the database, as it will be the same repetative tasks over and over the
    class it self will hold the actual queries
    """
    
    def __init__(self):
        print('Opening connection to the server')
        self.cnxn = create_engine("mysql+pymysql://root:#1Runner!!@localhost:3306/SpyInformation")
        
    def check_if_tables_exist(self):
        """just a true or false that the tables exist basically"""
        query_tables_exists = "SELECT table_name FROM information_schema.tables where table_schema='SpyInformation';"
        tables = self.cnxn.execute(query_tables_exists).fetchall()
        if(len(tables)>0):
            return True
        else:
            return False
        
    def get_most_recent_dates(self):
        """gets the most recent date to use for getting where to split the values in yahoo
        :return tuple: a tuple where the first is a date for the keywords, second is the date of pricing, third is the date of the divis
        """
        query_keyword = r"SELECT MAX(date_values) FROM spy_keywords"
        query_pricing = r"SELECT MAX(date_values) FROM spy_pricing"
        query_divis = r"SELECT MAX(date_values) FROM spy_divis"
        keyword_date = self.cnxn.execute(query_keyword).fetchall()[0][0]
        pricing_date = self.cnxn.execute(query_pricing).fetchall()[0][0]
        divis_date = self.cnxn.execute(query_divis).fetchall()[0][0]
        return keyword_date, pricing_date, divis_date
    
    def create_tables(self, df, table_name):
        """a method that will simply add the dataframe to the database assuming"""
        df.to_sql(table_name, self.cnxn)
        
    def append_to_database(self, df, table_name):
        """a method that appends data to the database"""
        df.to_sql(table_name, self.cnxn, if_exists="append")
        
    def return_table(self, table_name):
        """returns the table as a pandas dataframe"""
        return pd.read_sql("SELECT * FROM {}".format(table_name), self.cnxn)


#a = HandleDB()
#b = a.get_most_recent_date()
#print(b)     