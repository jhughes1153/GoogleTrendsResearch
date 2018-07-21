'''
Created on Jul 17, 2018

A module to bee run on the users end that will send and recieve things from the server

@author: Jack
'''

import socket
import pandas as pd
import os
import matplotlib.pyplot as plt

class ClientService():
    
    def __init__(self):
        """the basic construct of this class will be sending and recieving and converting
        to a pandas dataframe or simply to a csv I guess
        """
        
        self.dataframe = pd.DataFrame()
        
        self.input_string = ''
        
        #prompt the user for input at this point
        #=======================================================================
        # self.ticker = input('Please enter the stock ticker of the company:\n')
        # self.table_type = input('Please specify the type of table you would like to view for default simply leave this blank: possible options are: all prices, dividends, keywords\n')
        # self.keywords = input('If you are speicifying keywords please place them here in list order separated by commas in the form: a, b, c\n')
        #=======================================================================
        
        self.ticker = 'xom'
        self.table_type = 'keywords'
        self.keywords = 'dog, cat, xom'
        
        self.build_string()
        
        self.connect_to_server()
        
        while(True):
            next_command = input('possible options now that file created: visualize, export to excel, print file. Type quit to exit\n')
            if(next_command == 'quit'):
                break
            elif(next_command == 'visualize'):
                self.visualize_data()
            elif(next_command == 'export to excel'):
                self.to_excel()
            elif(next_command == 'print file'):
                self.print_dataframe()
            else:
                print('That is not a valid command')
            
            
            
        
    def build_string(self):
        #this will always be here pretty much
        temp = self.ticker
        
        #for this section we need to check and make sure we have valid inputs
        words = ['all prices', 'dividends', 'keywords']
        if(self.table_type in words):
            if(self.table_type == 'all prices'):
                temp = temp + ', all_prices'
            elif(self.table_type == 'dividends'):
                temp = temp + ', divis'
            else:
                temp = temp + ', ' + self.table_type
        else:
            print('did not enter valid table name, using default')
            temp = temp + ', ' + 'all_prices'
        
        print(temp)
        #this section will basically go in and make sure that everything is uniform
        #=======================================================================
        # str_b = ''
        # if(',' in self.keywords):
        #     temp_list = self.keywords.split(',')
        #     print(temp_list)
        # else:
        #     temp_list = [self.keywords]
        # for t in temp_list:
        #     if(t[0] == ' '):
        #         t = t[1:]
        #     elif(t[-1] == ' '):
        #         t = t[:-1]
        #     str_b = str_b + t + ', '
        # print(str_b)
        #=======================================================================
        
        if self.keywords != '':
            temp = temp +', ' + self.keywords
            
        #print(temp)
        
        self.input_string = temp
        
    def connect_to_server(self):
        """A test function that calls the server with the built string and then 
        gets the response and places it in the dataframe, assumes a json file 
        is what it is recieving
        """
        HOST, PORT = '192.168.0.18', 5000

        #create a socket (SOCK_STREAM means a TCP socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        try:
            #connect to the server and send data
            sock.connect((HOST, PORT))
            
            sock.sendall(bytes(self.input_string + '\n', 'utf-8'))
            
            json_string = ''
            count = 0
            #receive data from the server and shut down
            while True:
                #print(count)
                received = str(sock.recv(65536), 'utf-8')
                json_string = json_string + received
                if(received == ''):
                    break
                count += 1
                
            self.dataframe = pd.read_json(json_string, orient='records')
        finally:
            print('socket closed')
            sock.close()
            
    def to_excel(self):
        """A function I'm sure my dad will enjoy because its main purpose is simply to export a file to a csv
        which as we know can be opened in excel to look at, it should add these despite it being 
        """
        if(os.name == 'posix'):
            username = os.getenv('USER')
            if not os.path.exists('/home/' + username + 'googlekeywords/outputmatrices'):
                os.makedirs('/home/' + username + '/googlekeywords/outputmatrices')
            self.dataframe.to_csv('/home/' + username + '/googlekeywords/outputmatrices/' + self.ticker + 'matrix.csv', index=False)
            print('file exported, can be found in location /home/' + username + '/googlekeywords/' + self.ticker + 'matrix.csv')
        elif(os.name == 'nt'):
            username = os.getenv('USERNAME')
            if not os.path.exists(r'C:\Users\\' + username + '\\googlekeywords\\'):
                os.makedirs(r'C:\Users\\' + username + '\\googlekeywords\\')
            self.dataframe.to_csv(r'C:\Users\\' + username + '\\googlekeywords\\' + self.ticker + 'matrix.csv', index=False)
            print(r'C:\Users\\' + username + '\\googlekeywords\\' + self.ticker + 'matrix.csv')
        else:
            print('could not figure out your username or you are not using a windows or linux machine')
    
    def visualize_data(self):
        words = ''
        
        print('This section allows you to visualize the data by plotting points against the price and time')
        print('Possible words include:\n' + str(self.dataframe.columns.values))
        print('please enter a word or type quit to exit')
        while(True):
            words = input('word:')
            if(words == 'quit'):
                break
            words = words.split(', ')
            for word in words:
                if(word in self.dataframe.columns):
                    plt.plot(self.dataframe['timestep'], self.dataframe[word], label = word)
                else:
                    print('that word does not work please try one that exists')
            plt.plot(self.dataframe['timestep'], self.dataframe['closed'], label = 'Price')
            plt.xlabel('time')
            plt.ylabel('values')
            plt.legend(loc = 'best')
            plt.show()
            
            print('Possible words include:\n' + str(self.dataframe.columns.values))
            
            
        
            
    def print_dataframe(self):
        """A function that is used to print and return the dataframe
        """
        print(self.dataframe)
        

if __name__ == '__main__':
    client = ClientService()   
#test.connect_to_server()
#test.visualize_data()
#test.print_dataframe()





