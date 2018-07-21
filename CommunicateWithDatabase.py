'''
Created on Jul 10, 2018

This module only contains one class because I am still used to just one class per file, but this one is just one class
that handles reading and writing to the database, it does computations in itself which could be added to another 
object but no need to overcomplicate

@author: Jack
'''

import pandas as pd
import pyodbc
import datetime
#these here for testing reasons
#from DividendYahoo import DiviScrape
#from GoogleTrendsScrape import GoogleTrends


class HandleDatabase():
    """A class that will deal with writting and reading to and from the sql server
    the actual queries will be stored here as well as we want dynamic ones and I 
    know python better than sql
    
    issues
        Must if passed the string keyword, run the function resolved_dates in create_table so we can 
        get every date correct in the database
        
        weekly_after does not work if the first day does not match up
    """
    
    def __init__(self):
        print('Opening connection to the server')
        self.cnxn = pyodbc.connect(r'Driver={SQL Server};SERVER=DESKTOP-UMHFLD7;DATABASE=TestFinance;Trusted_Connection=yes;')
    
        self.cursor = self.cnxn.cursor()
        
    def create_table(self, dataframe, ticker, type_of):
        """A function that will write the dataframe data to the sql database, it does this by
        converting the dataframe into a json file and then invoking the sql openjson method, at
        the same time it also gets the columns of the dataframe and adds them to a string that 
        is the query, it then checks if the database is created 
        
        dataframe: pandas dataframe that will be added to the sql server
        ticker: stock ticker, so basically the name that will be added to the table
        type_of: string that will be the name of thing things, must be 1 of 3 basically, this will just
            be used for creating the dynamic SQL, The possible inputs need to be: all_prices, divis, keywords
        """
        
        def resolve_dates(dateslice):
            """A method that will pass a list and the point of this is to figure out if the dates in 
            the dataframe need to be updated
            
            dateslice: a list that is full of dates from the other dataframe
            """
            #date_list will store the dates from the query result
            #dates_correctd will be getting the correct values
            dates_corrected, date_list = [],[]
            
            query = 'SELECT DATES FROM FinTest.' + ticker + '_all_prices;'
            self.cursor.execute(query)
            
            for row in self.cursor.fetchall():
                temp = str(row)[2:-4]
                date_list.append(datetime.datetime.strptime(temp, '%Y-%m-%d'))
            
            for i in range(len(dateslice)):
                my_date = dateslice[i].to_pydatetime()
                if(dateslice[i] not in date_list):
                    temp = str(my_date + datetime.timedelta(days=1))
                    index = temp.find(' ')
                    dates_corrected.append(temp[:index])
                else:
                    temp = str(my_date)
                    index = temp.find(' ')
                    dates_corrected.append(temp[:index])
                    
            return dates_corrected  
        
        if(type_of[0] == 'k'):
            dates = dataframe['date']
            dataframe.drop('date', axis = 1, inplace = True)
            dataframe.insert(0, 'date', resolve_dates(dates))
        
        ticker = ticker + '_' + type_of
        
        #grab the column names of the dataframe
        column_names = dataframe.columns.values.tolist()
        
        #turn the dataframe into a json or string basically
        dataframe_json = dataframe.to_json(orient='records')
        
        columns_real = []
        for column in column_names:
            if(' ' in column):
                index = column.find(' ')
                column = column[:index] + '_' + column[index+1:]
            elif('&' in column):
                index = column.find('&')
                column = column[:index] + column[index+1:]
            columns_real.append(column)
        
        #start constructing the string that we will need to dynamically put the json table
        #into a new table of the database
        table_helper = 'WITH ('
        
        """We do this because the query needs to look like:
        WITH (
            column1 float $.column1, #float because all columns in the dataframe will be floats
            column2 float $.column2
            ...
        )
        Python is a good place to do this as it will be easier here than in sql
        """
        for column in columns_real:
            if(column == 'date'):
                table_helper = table_helper + 'DATES date \'$.date\','
            elif(column == 'timestep'):
                table_helper = table_helper + 'timestep int \'$.timestep\','
            else:
                table_helper = table_helper +' ' + column + ' float \'$.' + column + '\','
        
        #do this to add the final ')' and get rid of the extra comma
        table_helper = table_helper[:-1] + ')'
        
        query = 'DECLARE @json nvarchar(max) = N\'' + dataframe_json + '\' IF OBJECT_ID(\'FinTest.' + ticker + '\', \'U\') IS NOT NULL DROP TABLE FinTest.' + ticker + ' SELECT * INTO FinTest.' + ticker + ' FROM OPENJSON(@json) ' + table_helper + ';'
               
        self.cursor.execute(query)
        
        self.cursor.commit()
        
        self.cursor.execute('SELECT TOP 3 * FROM FinTest.' + ticker)
           
        for row in self.cursor.fetchall():
            print(row)
           
        #cnxn.close()
        
    def get_table(self, table_name):
        """This is here because I guess MSSQL doesnt like this type of 
        thing because update statements can be used and that can be a 
        security issue
        
        This function exists to simply get a whole table from a database with
        no changes made to it
        """
        
        query = 'SELECT * FROM FinTest.' + table_name + ' FOR JSON AUTO'
        
        self.cursor.execute(query)
        
        #we have to build the json sting because it comes in chunks
        json_in = ''
        
        for row in self.cursor.fetchall():
            #do this because we need to change the string later
            temp = str(row)
            #crop off the extra stuff that we have got from sql
            temp = temp[2:-4]
            #built a massive string that is the json file
            json_in = json_in + temp
        
        table = pd.read_json(json_in, orient='records')
        
        return table
    
    def get_dividends(self, ticker):
        """this function gets the relevant info about dividends from the database such as if it covered
        or not, this is more about a functionality of a different project but still makes sense in this
        one as stock price is adjusted based on dividends
        """
        
        #get the right tables
        ticker_yahoo = 'FinTest.' + ticker + '_all_prices'
        ticker_google = 'FinTest.' + ticker + '_divis'
        
        '''this may be hard coded but these tables will have these values until yahoo changes their api
        also we insert into a temptable to help with getting the JSON properly
        premise: insert the join query into a temptable, then get the temptable as a table as the normal
        joined table ended up being a nested table within the json file and thats not what we want
        '''
        query = 'SELECT ' + ticker_google + '.*, opened, high, low, closed, volume INTO #temptable FROM ' + ticker_yahoo + ' JOIN ' + ticker_google + ' ON ' + ticker_yahoo + '.DATES = ' + ticker_google + '.DATES'
        
        self.cursor.execute(query)
        
        #gets teh values from the temptable
        query_get_table = 'SELECT * FROM #temptable FOR JSON AUTO;'
        
        self.cursor.execute(query_get_table)
        #have to piece together the chunked data
        json_in = ''
        
        for row in self.cursor.fetchall():
            #do this because we need to change the string later
            temp = str(row)
            #crop off the extra stuff that we have got from sql
            temp = temp[2:-4]
            #built a massive string that is the json file
            json_in = json_in + temp
        
        table = pd.read_json(json_in, orient='records')
        
        days_before, covered = [],[]
        dates = table['DATES']
        for i in range(len(dates)):
            temp = datetime.datetime.strptime(dates[i], '%Y-%m-%d')
            day_before = str(temp - datetime.timedelta(days=1))
            index = day_before.find(' ')
            day_before = (day_before[:index])
            query_get_date_before_ex = 'SELECT closed FROM ' + ticker_yahoo + ' WHERE DATES = \'' + day_before + '\';'
            self.cursor.execute(query_get_date_before_ex)
            result = self.cursor.fetchone()[0]
            days_before.append(result)
            adjusted_close = result - table['amount'][i]
            covered.append(table['high'][i] - adjusted_close)
        
        table['close_day_before'] = days_before
        table['covered'] = covered
        
        print(table.shape)
        
        return table
        
    
    def get_keywords_weekly(self, ticker):
        """this function basically does what the GoogleTrendsScrape used to do
        it takes values from yahoo and combines them with the google trends data
        it does this by doing a join query on the google trends data and the 
        yahoo price data
        """
        
        #get the right tables
        ticker_yahoo = 'FinTest.' + ticker + '_all_prices'
        ticker_google = 'FinTest.' + ticker + '_keyword'
        
        '''this may be hard coded but these tables will have these values until yahoo changes their api
        also we inset into a temptable to help with getting the JSON properly
        premise: insert the join query into a temptable, then get the temptable as a table as the normal
        joined table ended up being a nested table within the json file and thats not what we want
        '''
        query = 'SELECT ' + ticker_google + '.*, opened, high, low, closed, volume INTO #temptable FROM ' + ticker_yahoo + ' JOIN ' + ticker_google + ' ON ' + ticker_yahoo + '.DATES = ' + ticker_google + '.DATES'
        
        self.cursor.execute(query)
        
        #gets teh values from the temptable
        query_get_table = 'SELECT * FROM #temptable FOR JSON AUTO;'
        
        self.cursor.execute(query_get_table)
        #have to piece together the chunked data
        json_in = ''
        
        for row in self.cursor.fetchall():
            #do this because we need to change the string later
            temp = str(row)
            #crop off the extra stuff that we have got from sql
            temp = temp[2:-4]
            #built a massive string that is the json file
            json_in = json_in + temp
        
        table = pd.read_json(json_in, orient='records')
        
        print(table.shape)
        
        return table
            
        """ADD CODE TO RETURN THIS AS A JSON AND NOT AN SQL TABLE AS THATS USELESS BASICALLY
        """
            
    def weekly_after(self, ticker, dataframe=pd.DataFrame()):
        """An attempt at an easier way to change the dimensions, this will be a slow function most likely
        ticker is the name of the stock one would like to use
        """
        
        if dataframe.empty:
            #we are going to use this to get the dates for the values, might change it to a query later
            data = self.get_table(ticker + '_keyword')
            dates = data["DATES"]
        else:
            data = dataframe
            dates = data['date']
        
        #all the queries will start with this string
        query_start = 'SELECT TOP 5 DATES, closed FROM FinTest.' + ticker + '_all_prices '
        
        a,b,c,d,e = [],[],[],[],[]
        
        #for all dates 
        for date in dates:
            start_date_temp = datetime.datetime.strptime(date, '%Y-%m-%d')
            end_date_temp = start_date_temp + datetime.timedelta(days=5)
            start_date = str(start_date_temp)[:10]
            end_date = str(end_date_temp)[:10]
            
            #need to reset it per loop so that we dont keep adding to it
            query = ''
            query = query_start + 'WHERE DATES BETWEEN \'' + start_date + '\' AND \'' + end_date + '\';'
            
            self.cursor.execute(query)
            
            count = 0
            closed = []
            for row in self.cursor.fetchall():
                #get the date and convert the given string to a dateobject
                date = datetime.datetime.strptime(row[0], '%Y-%m-%d')
                #this is to make sure that the dates line up for the 5 days starting on the correct day
                #it should work even if the first value isnt there
                temp = start_date_temp + datetime.timedelta(days=count)
                #add a zero for the moment if the dates dont add up
                print(date, ':', temp)
                if(date != temp):
                    for i in range(1,5):
                        count += 1
                        temp = temp + datetime.timedelta(days=count)
                        print(temp)
                        if(date == temp):
                            closed.append(row[1])
                            break
                        if(i == 4):
                            closed.append(0)
                    #count += 1
                else:
                    closed.append(row[1])
                    count += 1
            
            #need this here basically to make sure that the array was 5 values no matter what
            while(len(closed) != 5):
                closed.append(0)
            
            #The lazyman way of doing this but it adds the correct values from above into the arrays
            a.append(closed[0])
            b.append(closed[1])
            c.append(closed[2])
            d.append(closed[3])
            e.append(closed[4])
            
        data['1st_day'] = a
        data['2nd_day'] = b
        data['3rd_day'] = c 
        data['4th_day'] = d
        data['5th_day'] = e
        
        data.to_csv(r'C:\Users\Jack\a.csv')
        
        return data
    
    def move_prices_weekly(self, ticker, dataframe=pd.DataFrame()):
        """This function actually works, it takes the price column from the full table
        and transposes them basically to a new matrix split on every 5 days, but also adds
        a zero if the market was not open for the whole week
        """
        
        if dataframe.empty:
            #we are going to use this to get the dates for the values, might change it to a query later
            data = self.get_table(ticker + '_keyword')
            dates = data["DATES"]
        else:
            data = dataframe
            dates = data['date']
        
        #all the queries will start with this string
        query_start = 'SELECT TOP 5 DATES, closed FROM FinTest.' + ticker + '_all_prices '
        
        a,b,c,d,e = [],[],[],[],[]
        
        #for all dates 
        for date in dates:
            start_date_temp = datetime.datetime.strptime(date, '%Y-%m-%d')
            end_date_temp = start_date_temp + datetime.timedelta(days=5)
            start_date = str(start_date_temp)[:10]
            end_date = str(end_date_temp)[:10]
            
            #need to reset it per loop so that we dont keep adding to it
            query = ''
            query = query_start + 'WHERE DATES BETWEEN \'' + start_date + '\' AND \'' + end_date + '\';'
            
            self.cursor.execute(query)
            
            temp_dates = []
            temp_closed = []
            closed = []
            rows = self.cursor.fetchall()
            for row in rows:
                #get the date and convert the given string to a dateobject
                date = datetime.datetime.strptime(row[0], '%Y-%m-%d')
                #this is to make sure that the dates line up for the 5 days starting on the correct day
                temp_dates.append(date)
                temp_closed.append(row[1])
                
            #This is 5 because the stock market is only open 5 days a week
            count = 0   
            for i in range(5):    
                #get the temp date based on the starting date
                temp = start_date_temp + datetime.timedelta(days=i)
                #if the temp date is in the rows then add the closed price
                if(temp in temp_dates):
                    closed.append(temp_closed[count])
                    #do the count here so that we dont get an out of bounds
                    count += 1
                else:
                    closed.append(0)
            
            #need this here basically to make sure that the array was 5 values no matter what
            while(len(closed) != 5):
                closed.append(0)
            
            #The lazyman way of doing this but it adds the correct values from above into the arrays
            a.append(closed[0])
            b.append(closed[1])
            c.append(closed[2])
            d.append(closed[3])
            e.append(closed[4])
            
        data['closed'] = a
        data['2nd_day'] = b
        data['3rd_day'] = c 
        data['4th_day'] = d
        data['5th_day'] = e
        data['timestep'] = list(range(0, len(dates)))
        
        data.to_csv(r'C:\Users\Jack\a.csv')
        
        return data
                
    def check_exists(self, ticker, table_type):
        """checks if the given table is a valid one in the database
        """
        
        query = 'IF OBJECT_ID (N\'FinTest.' + ticker + '_' + table_type +'\', N\'U\') IS NOT NULL SELECT \'True\'; ELSE SELECT \'False\';'
        
        self.cursor.execute(query)
        
        exists = self.cursor.fetchone()[0]
        
        if(exists == 'True'):
            return True
        else:
            return False
        
    def close_connection(self):
        """close the connection when done with the object
        """
        self.cnxn.close()    
        print('closed connection to SQL server') 
        

if __name__ == '__main__':
    db1 = HandleDatabase()
    #===========================================================================
    # dow_stocks = ['mmm', 'axp', 'aapl', 'ba', 'cat', 'cvx', 'csco', 'ko', 'dis', 'dwdp', 'xom', 'gs', 'hd', 'ibm', 'intc', 'jnj', 'jpm', 'mcd', 'mrk', 'msft', 'nke', 'pfe', 'pg', 'trv', 'utx', 'unh', 'vz', 'v', 'wmt', 'wba']
    # for stock in dow_stocks:
    #     print('getting values for ' + stock)
    #     stock_values = DiviScrape(stock)
    #     db1.create_table(stock_values.return_dataframes()[0], stock, 'all_prices')   
    #     db1.create_table(stock_values.return_dataframes()[1], stock, 'divis')
    #===========================================================================
    #stock_values = DiviScrape('aapl')
    #db1.create_table(stock_values.return_dataframes()[1], 'aapl', 'divis')
    #a = db1.get_dividends('aapl')
    #print(a)
    
    #dont comment out this one
    db1.close_connection()      
        
            
            
        
        
        