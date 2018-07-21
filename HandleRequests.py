'''
Created on Jul 17, 2018

@author: Jack
'''

import socketserver
from CommunicateWithDatabase import HandleDatabase
from DividendYahoo import DiviScrape
from GoogleTrendsScrape import GoogleTrends

class MyTCPHandler(socketserver.StreamRequestHandler):
    """
    The request handler class for the server
    
    It is instantiated once per connection to the server,
    and must override the handle() method to implement communication
    to the client
    """
    
    def handle(self):
        #write to a local logfile
        self.logfile = open(r'C:\Users\Jack\googlekeywords\ServerLog\log.txt', 'a')
        try:
            #read commands from the client even if it has to come over many different packets
            self.data = self.rfile.readline().strip()
            #strip the extra shit off
            self.data = str(self.data)[2:-1]
            #these are for logging info about the server while it is running
            self.logfile.write('{} connected to server\n'.format(self.client_address[0] ))
            self.logfile.write('requested this line: ' + self.data + '\n')
            
            rc = RequestController(self.data)
            json_back = bytes(rc.return_json(), 'utf-8')
            
            #just send back the same data, but upper-cased
            self.request.sendall(json_back)
            self.logfile.write('Successfuly sent json to client\n')
        except Exception as e:
            #log errors
            self.logfile.write('===ERROR===\n')
            print(e)
            self.logfile.write(str(e))
            
            
        
class RequestController():
    """A class that will be used to handle the messages that are passed to and from the server
    
    messages to this server will be handles as: <ticker>, <table_type>
        if table_type is left empty just return the one of all the prices
    
    All functions will return a string or a json which is still a string
    """
    
    def __init__(self, command_list):
        #we are basically using this to construct a json in some dynamic way
        self.json_string = ''
        
        #create the database handler object
        self.dbhandler = HandleDatabase()
        
        #if it is just one word that skip ahead
        if(',' in command_list):
            #split the values on the comma separated list
            command_list = command_list.split(', ')
            #if thre are only two values we may just be querying for new info
            if(len(command_list) == 2):
                self.ticker = command_list[0]
                self.table_type = command_list[1]
                #get keywords if the table type is keywords
                if(self.table_type == 'keywords'):
                    #use this because its easy to check if its empty, it is needed below this and for the function
                    self.keywords_list = []
                    self.resolve_google_keywords()
                else:
                    self.resolve_yahoo_table()
                self.resolve_yahoo_table()
            #this is for if we are passed a list of keywords to make a new table
            elif(len(command_list) > 2):
                self.ticker = command_list[0]
                self.table_type = command_list[1]
                self.keywords_list = command_list[2:]
                self.resolve_google_keywords()
        #if its just the prices we want this one, its like a default
        else:
            self.ticker = command_list
            #this will be the default
            self.table_type = 'all_prices'    
            self.resolve_yahoo_table()  
            
        self.dbhandler.close_connection()
    
    def resolve_yahoo_table(self):
        """This method will figure out if the table exists, if it does not then it will call the relevant
        functions to create the table in the database
        """
        
        exists = self.dbhandler.check_exists(self.ticker, self.table_type)
        
        #if the table exists return it, if not then return nothing at the moment
        if(exists):
            table = self.dbhandler.get_table(self.ticker + '_' + self.table_type)
            self.json_string = table.to_json(orient = 'records')
        else:
            yahoo_info = DiviScrape(self.ticker)
            yahoo_tuple = yahoo_info.return_dataframes()
            self.dbhandler.create_table(yahoo_tuple[0], self.ticker, 'all_prices')
            self.dbhandler.create_table(yahoo_tuple[1], self.ticker, 'divis')
            table = self.dbhandler.get_table(self.ticker + '_' + self.table_type)
            self.json_string = table.to_json(orient = 'records')
        
    def resolve_google_keywords(self):
        """This function will be called if there are keywords present or if the table type was keywords
        """
        
        exists = self.dbhandler.check_exists(self.ticker, self.table_type)
        data_exists = self.dbhandler.check_exists(self.ticker, 'all_prices')
        
        #we need to do this because one of the functions that we call needs to have
        #the prices table, so this will solve that
        if(not data_exists):
            print('need to get other info')
            yahoo_info = DiviScrape(self.ticker)
            yahoo_tuple = yahoo_info.return_dataframes()
            self.dbhandler.create_table(yahoo_tuple[0], self.ticker, 'all_prices')
            self.dbhandler.create_table(yahoo_tuple[1], self.ticker, 'divis')
        
        if(len(self.keywords_list) > 0):
            #create new googletrends object and it will automatically do the things in the list
            gt = GoogleTrends(self.ticker, self.keywords_list)
            #get the dataframe into this program
            google_trends_frame = gt.return_dataframe()
            #create a new table with the dataframe
            self.dbhandler.create_table(google_trends_frame, self.ticker, 'keywords')
            #do that magic with it starting at 
            table = self.dbhandler.move_prices_weekly(self.ticker, google_trends_frame)
            self.json_string = table.to_json(orient='records')
        else:
            if(exists):
                table = self.dbhandler.move_prices_weekly(self.ticker)
                self.json_string = table.to_json(orient='records')
            else:
                print('not quite sure what you want to do maybe passed in a blank keyword list by accident')
    
    def return_json(self):
        """a function that returns the instance variable
        """
        return self.json_string
        
        
if __name__ == '__main__':
    #setup the host and port for the server
    HOST, PORT = '192.168.0.18', 5000
    
    #create the server, binding to localhost on the port
    server = socketserver.TCPServer((HOST, PORT), MyTCPHandler)
    
    #activate the server, this will run until you interupt it
    server.serve_forever()
    
    
    
    
    
    