'''
Created on Jun 27, 2018

later when using a database make sure to do a right join so that
when this runs automatically every week that only the new values get entered

@author: jack
'''
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time
import os
import datetime

class GoogleTrends:
    """A class that will be used to extract data from google and puts
    it into a pandas dataframe, it can export to a csv file and can also return
    the dataframe itself if it needs to be used in another program
    """
    
    def __init__(self, quote, keywords):
        '''Constructor, creates a pandas dataframe that will later
        be used to add to a database for reference instead of keeping
        many csv files, but keeping it as a pandas dataframe will 
        help make this easier for now
        params: string that is a stock quote
        '''
        self.keywords = pd.DataFrame()
        self.quote = quote
        self.recurse_break = 0
        self.state = 0
        print('Creating object for ' + quote)
        self.scrape_data(keywords)

    def scrape_data(self, keywords):
        """This method uses selenium to open a page and get the value
        from google trends by running the javascript, it then takes
        it and adds the values to an already made csv file, this is 
        a bit slow because it has to load a new webpage through firefox
        on the current pc
        Params: accepts a single string to check, cant get the html to
            work properlly with this, damn
            
        right now this is set up so that if you run once it gets all values
        passed in, so if you are updating the keywords it is not corrected yet
        """
        
        def new_keywords(keyword):
            """This method is here in order to add the %20 to the keywords in the list
            %20 is a space in googles queries
            This little snippet is so that I dont need to put the %20 myself cause
            that is annoying, if the string has a space then it will add this to
            a new var, and it just keeps the old keyword if no space is present, basically
            I can just pass in a list of strings and if they have a space itll add a new one
            but it assumes only one space for the entire string, can add more later if it
            ends up needing to, maybe n spaces will be needed
            params: a single string
            returns: a new keyword that adds %20 instead of spaces
            """
            new_keyword = ''
            for i in range(len(keyword)):
                if(keyword[i] == ' '):
                    new_keyword = new_keyword + '%20'
                else:
                    new_keyword = new_keyword + keyword[i]
            return new_keyword
        
        def add_blank_column(keyword):
            """this might need to be run if either the program doesn't return values of the right
            shape or if it does not find the correct values, so this adds a column of zeros to 
            the dataframe if it cant get the url
            params: accepts a string and makes that the name of the column
            """
            
            nums = np.zeros(len(self.keywords.price))
            self.keywords[keyword] = nums
        
        
        #This is to set it so that selenium runs in headless and wont up
        #a new browser window
        options = Options()
        options.set_headless(headless=True)
        #I realize this is lazy but it will change it once I remever the proper way to do it
        driver = webdriver.Firefox(executable_path='/home/jack/Downloads/geckodriver-v0.23.0-linux64/geckodriver')

        #if only passed 1 value then make make it a list still
        if isinstance(keywords, str): keywords = [keywords]
        #iterate over keywords
        for keyword in keywords:
            print('getting trends for', keyword)
            if keyword not in self.keywords.columns:
                
                new_keyword = new_keywords(keyword)
                
                #this code here to make sure that we get the url and it will keep trying until we get it
                breakout = 0
                while(breakout < 3):
                    try:
                            #grab the html and enable the javascript
                        driver.get('http://trends.google.com/trends/explore?date=today%205-y&geo=US&q=' + new_keyword)
                        
                        #sleep to make sure that the js executes
                        time.sleep(6)
        
                        ##put the html into a string basically
                        html = driver.page_source
    
                        ##Put the string into beautiful soup
                        soup = BeautifulSoup(html, 'lxml')
                        
                        #find all tables 
                        table = soup.find('table')
                        
                        #find the values in the table
                        values = table.find_all('td')
                        
                        #add the values to python lists, could use numpy to speed up,
                        #but it is only 261 long always so why not use that
                        count = 0
                        dates, dates_conv, nums = [],[],[]
                        
                        for value in values:
                            if(count % 2 == 0):
                                if(self.state == 1):
                                    pass
                                else:
                                    dates.append(value.getText()[1:-1])
                            else:
                                nums.append(value.getText())
                            count += 1
                            
                        #state machine for the dates
                        if(self.state == 0):
                            """We need this loop because the dates come in a different format than yahoo
                            so to do sql this needs to be changed and we need to add a day since google 
                            updates the values on sunday, and the market opens monday
                            """
                            for i in range(len(dates)):
                                #print(type(dates[0]))
                                my_date = datetime.datetime.strptime(dates[i], '%b %d, %Y')
                                #print(type(my_date))
                                dates_conv.append(my_date + datetime.timedelta(days=1))
                            self.keywords['date'] = dates_conv
                            self.state = 1    
                        
                        #try this just in case it is a different size in the file
                        if(len(nums) == len(self.keywords.date)):
                            self.keywords[keyword] = nums
                            #print(self.keywords.head())
                        else:
                            print('column not the right shape, using zeros')
                            add_blank_column(keyword)
                        breakout = 3
                        
                    except Exception as ex:
                        print(ex)
                        #if did not get url add 1 to breakout so we arent in a loop for a url that does not exist
                        breakout += 1
                        if(breakout < 3):
                            #make a not in the file and wait
                            print('trying again after waiting for 5 seconds')
                            time.sleep(5)
                        else:
                            #make a note that we could not get the url and then add a column of zeros to it
                            add_blank_column(keyword)
                            print('Skipping column could not get url')
                            
                
            else:
                #just print as we do not really care if the keyword is already in the file as it most likley will not be
                print('That keyword already exists')
        driver.quit()
                
    def return_dataframe(self):
        """This function here to get the dataframe after all values have been
        added so that eventually we can just add the dataframe to the database
        """
        return self.keywords
    
    def print_head(self):
        """A function that helps with finding out info about the dataframe that
        is created
        """
        print(self.keywords.head())
        print('shape of the dataframe:', self.keywords.shape)
        
    def keywords_to_csv(self):
        """This function no longer needed in this program as it makes more sense elsewhere and is currenlty
        handled elsewhere
        
        This function is for outputing to a csv file, but does not work if the file does not exist so
        the directory must be set up prior to it being available
        """
        if(os.name == 'posix'):
            username = os.getenv('USER')
            if not os.path.exists('/home/' + username + 'googlekeywords/'):
                os.makedirs('/home/' + username + '/googlekeywords/')
            self.keywords.to_csv('/home/' + username + '/googlekeywords//' + self.quote + 'matrix.csv', index=False)
        elif(os.name == 'nt'):
            username = os.getenv('USERNAME')
            self.keywords.to_csv('C:\\Users' + username + '\\googlekeywords\\\\' + self.quote + 'matrix.csv', index=False)
        else:
            print('could not figure out your username or you are not using a windows or linux machine')
            
        #close the opened file
            

if __name__ == '__main__':
    #use this in order to get it to ask the person for the words and the stock
    #===========================================================================
    # stock = input('Please enter a stock ticker:\n')
    # 
    # keys = input('Please enter the keywords that you would like to use and please use commas to separate words:\n')
    # keys = keys.split(',')
    # for i in range(len(keys)):
    #     if(' ' in keys[i]):
    #         keys[i] = keys[i][1:]
    #===========================================================================
     
    googleTrendsScrape = GoogleTrends('aapl', ['Dividend'])
     
    #keywords_for_ford = ['Dividend']
    #googleTrendsScrape.scrape_data()
    googleTrendsScrape.print_head()
    googleTrendsScrape.keywords_to_csv()

