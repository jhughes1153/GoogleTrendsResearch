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
        """a method that will simply add the dataframe to the database assuming
        :param df: dataframe
        :param table_name: string for table name
        """
        df.to_sql(table_name, self.cnxn)
        
    def append_to_database(self, df, table_name):
        """a method that appends data to the database
        :param df: dataframe
        :param table_name: string for table name
        """
        df.to_sql(table_name, self.cnxn, if_exists="append", index=False)
        
    def return_table(self, table_name):
        """returns the table as a pandas dataframe
        :param table_name: string for the table
        """
        return pd.read_sql("SELECT * FROM {}".format(table_name), self.cnxn)

    def delete_from_table(self, table_name, day):
        """
        deletes from the table for a certain day
        :param table_name: name of the table to delete from
        :param day: string for the day to delete
        """
        self.cnxn.execute("DELETE FROM {} WHERE DATE(date_values) = '{}'".format(table_name, day))
