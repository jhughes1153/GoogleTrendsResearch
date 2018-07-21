'''
Created on Jul 5, 2018

A module that contains a class that will be used to webscrape from yahoo

@author: jack
'''

import requests
import time
import pandas as pd
import datetime
import json
from bs4 import BeautifulSoup

class DiviScrape():
    """A class that gets historic stock data and historic dividends data, it can handle stock splits as well
    """
    
    def __init__(self, quote):
        """quote is a string that is a stock ticker symbol that is passed in
        divi_df is a dataframe that contains the data fram the yahoo json file
        """
        self.quote = quote
        self.data_df = pd.DataFrame()
        self.divi_df = pd.DataFrame()
        self.scrape_yahoo()
        
    def scrape_yahoo(self):
        """A function that is going to just return a giant dataframe of yahoo data for the last 5 years
        """
        
        def resolve_url():
            """This method is here to resolve the url as it uses the unix timestamp so this gets the current one
            and the unix timestamp from 5 years ago as well, it returns the string with the proper url
            returns: returns a string that is the url with the correct times
            """
            current = int(time.time())
            current -= current % -100
            five_years = current - (31557600 * 5) + 604800
            five_years -= five_years % -100
            return 'https://finance.yahoo.com/quote/' + self.quote + '/history?period1=' + str(five_years) + '&period2=' + str(current) +'&interval=1d&filter=history&frequency=1d'
        
        def parse_json(input_string):
            """This method is only to be called from the scrape_yahoo
            method used to extract the json file with all of
            the info from the html, it should dynamically grab the json
            and exract all the relevant info, it is to be kept as a string.
            Returns: a pandas dataframe with the open, close, and prices
            Params: a string to be parsed for the json file, this is a 
            nested function as there are no private methods in python
            returns: returns a dataframe with the values needed
            """                
            
            hist = input_string.find('HistoricalPriceStore')
            
            hist_table = input_string[hist:]
            #starting value of the json file
            index_a = hist_table.find('{')
            #ending value, basically I'll just add the ending characters
            index_b = hist_table.find(']')
            hist_b = hist_table[index_a:index_b] + ']}'
            
            fileout = open(r'C:\Users\Jack\text.txt', 'w')
            fileout.write(hist_b)
            fileout.close()
            
            price_values = json.loads(hist_b)
            
            #initialize some lists
            timestamps, opens, high, low, closed, volume, amount, dividend_timestamps = [],[],[],[],[],[],[],[]
            #add them to a mock matrix for later use
            matrix = [timestamps, opens, high, low, closed, volume, amount, dividend_timestamps]
            
            split_ratio = 1
            for result in price_values['prices']:
                #print(result)
                if('splitRatio' in result):
                    '''For some reason the numerator and the denominator are flipped in yahoos api so that is why these seem backwards
                    '''
                    denominator = result[u'numerator']
                    numerator = result[u'denominator']
                    split_ratio = split_ratio * float(numerator) / float(denominator)
                    #amount = fix_dividend_values(numer, denom, amount)
                elif('amount' in result):
                    #we pass this because the json file keeps track of dividends, may want to use this in this file
                    #because this will tell us about dividiend stocks
                    amount.append(result[u'amount'] / split_ratio)
                    #this also comes as a unix timestamp
                    dividend_timestamps.append(result[u'date'])
                else:
                    #this comes as a unix timestamp
                    timestamps.append(result[u'date'])
                    opens.append(result[u'open'])
                    high.append(result[u'high'])
                    low.append(result[u'low'])
                    closed.append(result[u'close'])
                    volume.append(result[u'volume'])
                    #adj_close.append(result[u'adjclose'])
            
            #we reverse the string because in google the dates go the other direction    
            for i in range(len(matrix)):
                matrix[i].reverse()
    
            #unix timesteps are the number of seconds since 1970 something
            for i in range(len(timestamps)):
                #this converts them to a readable format
                timestamps[i] = datetime.datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')
            
            #same thing for this list as well   
            for i in range(len(dividend_timestamps)):
                dividend_timestamps[i] = datetime.datetime.fromtimestamp(dividend_timestamps[i]).strftime('%Y-%m-%d')
            
            stock_info = pd.DataFrame()
            divi_info = pd.DataFrame()
            stock_info['date'] = timestamps
            stock_info['opened'] = opens
            stock_info['high'] = high
            stock_info['low'] = low
            stock_info['closed'] = closed
            stock_info['volume'] = volume
            #adjusted closed will be calculated later once we have the relevant dataframes and such
            #stock_info['adj_close'] = adj_close
            
            divi_info['date'] = dividend_timestamps
            divi_info['amount'] = amount

            #this is to help with visualizing the data and also for
            #when I can figure out an RNN
            stock_info['timestep'] = list(range(0, len(volume)))
            
            self.data_df = stock_info
            
            self.divi_df = divi_info
        
        #call resolve_url to get the proper timestamps
        request = requests.get(resolve_url())
        
        html = request.text
        
        soup = BeautifulSoup(html, 'lxml')
        
        testing = soup.find_all('script')
        
        for script in testing:
            temp = str(script)
            if('(function (root)' in temp):
                parse_json(temp)
                #self.divi_df = temp_frame
                
    def print_dataframe_heads(self):
        """Take a small visual into the dataframes
        """
        print('Dividend Information:')
        print(self.divi_df.shape)
        print(self.divi_df)
        print('Stock information:')
        print(self.data_df.shape)
        print(self.data_df.head())
        
    def return_dataframes(self):
        """A function that returns the dataframe for the stock and 
        the one for the dividends
        """
        return self.data_df, self.divi_df
                    
#===============================================================================
# if __name__ == '__main__':
#     a = DiviScrape('aapl')
#     a.print_dataframe_heads()
#===============================================================================
        
        
        
        
        
        
        