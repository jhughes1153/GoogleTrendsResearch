import datetime as dt
import time
import logging

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import requests
import json
import os

_current_year = int(dt.datetime.now().strftime('%Y'))
logging = logging.getLogger(__name__)


def google_trends(keywords):
    """This method uses selenium to open a page and get the value
    from google trends by running the javascript, it then takes
    it and adds the values to an already made csv file, this is
    a bit slow because it has to load a new webpage through firefox
    on the current pc
    :Param keywords: accepts a single string to check, cant get the html to
        work properlly with this, damn

    right now this is set up so that if you run once it gets all values
    passed in, so if you are updating the keywords it is not corrected yet
    """

    def new_keywords(key):
        """This method is here in order to add the %20 to the keywords in the list
        %20 is a space in googles queries
        This little snippet is so that I dont need to put the %20 myself cause
        that is annoying, if the string has a space then it will add this to
        a new var, and it just keeps the old keyword if no space is present, basically
        I can just pass in a list of strings and if they have a space itll add a new one
        but it assumes only one space for the entire string, can add more later if it
        ends up needing to, maybe n spaces will be needed
        :param keyword: a single string
        :return new_keyword: a new keyword that adds %20 instead of spaces
        """
        safe_keyword = ''
        for i in range(len(keyword)):
            if keyword[i] == ' ':
                safe_keyword = safe_keyword + '%20'
            else:
                safe_keyword = safe_keyword + key[i]
        return safe_keyword

    keywords_df = pd.DataFrame()

    # This is to set it so that selenium runs in headless and wont up
    # a new browser window
    options = Options()
    options.set_headless(headless=True)
    gecko_path = f"{os.path.dirname(os.path.abspath(__file__))}/extras/geckodriver"
    driver = webdriver.Firefox(executable_path=gecko_path, options=options)

    # if only passed 1 value then make make it a list still
    if isinstance(keywords, str):
        keywords = [keywords]
    # iterate over keywords
    initial_state = True
    for keyword in keywords:
        logging.info(f'getting trends for {keyword}')
        print(f'getting trends for {keyword}')
        new_keyword = new_keywords(keyword)

        # this code here to make sure that we get the url and it will keep trying until we get it
        breakout = 0
        while breakout < 3:
            try:
                # grab the html and enable the javascript
                # https://trends.google.com/trends/explore?date=now%201-d&geo=US&q=amd
                driver.get('http://trends.google.com/trends/explore?date=now%201-d&geo=US&q={}'.format(new_keyword))

                # sleep to make sure that the js executes
                time.sleep(10)

                # #put the html into a string basically
                html = driver.page_source

                # #Put the string into beautiful soup
                soup = BeautifulSoup(html, 'lxml')

                # find all tables
                table = soup.find('table')

                temp_df = pd.read_html(str(table))[0]

                temp_df = temp_df.rename(index=str, columns={'x': 'date_values',
                                                             'y1': keyword.replace(' ', '_').replace('&', 'and')})
                temp_df['date_values'] = temp_df['date_values'].str[1:-1]
                temp_df['date_values'] = pd.to_datetime(temp_df['date_values'], format='%b %d at %I:%M %p').apply(
                    lambda x: x.replace(year=_current_year))
                logging.info('{} is of shape {}'.format(new_keyword, temp_df.shape))

                if initial_state:
                    keywords_df = temp_df
                    initial_state = False
                else:
                    keywords_df = keywords_df.merge(temp_df, on='date_values')
                breakout = 3
            except Exception as ex:
                logging.error(ex)
                # if did not get url add 1 to breakout so we arent in a loop for a url that does not exist
                breakout += 1
                if breakout < 3:
                    # make a not in the file and wait
                    logging.info('trying again after waiting for 5 seconds')
                    time.sleep(5)
                else:
                    # make a note that we could not get the url and then add a column of zeros to it
                    keywords_df[keyword.replace(' ', '_').replace('&', 'and')] = np.zeros(
                        len(keywords_df.date_values))
                    logging.warning('Skipping column could not get url')
    driver.quit()

    return keywords_df


def resolve_url(quote):
    """This method is here to resolve the url as it uses the unix timestamp so this gets the current one
    and the unix timestamp from 5 years ago as well, it returns the string with the proper url
    returns: returns a string that is the url with the correct times
    :return string: a url that is the correct one in yahoo
    """
    current = int(time.time())
    current -= current % -100
    last_week = current - (86400 * 7)
    last_week -= last_week % -100
    return 'https://finance.yahoo.com/quote/' + quote + '/history?period1=' + str(
        last_week) + '&period2=' + str(current) + '&interval=1d&filter=history&frequency=1d'


def get_basic_url(quote):
    """This method is used to just get the basic yahoo finance url as we dont need to get a custom tailored one
    as it might be less suspicious
    :return basic url: string
    """
    return 'https://finance.yahoo.com/quote/{stock}/history?p={stock}'.format(stock=quote)


def parse_json(input_string):
    """This method is only to be called from the scrape_yahoo
    method used to extract the json file with all of
    the info from the html, it should dynamically grab the json
    and exract all the relevant info, it is to be kept as a string.
    Returns: a pandas dataframe with the open, close, and prices
    Params: a string to be parsed for the json file, this is a
    nested function as there are no private methods in python
    :param input_string: json as a string
    :return returns a dataframe with the values needed
    """

    hist = input_string.find('HistoricalPriceStore')

    hist_table = input_string[hist:]
    # starting value of the json file
    index_a = hist_table.find('{')
    # ending value, basically I'll just add the ending characters
    index_b = hist_table.find(']')
    hist_b = hist_table[index_a:index_b] + ']}'

    price_values = json.loads(hist_b)

    # initialize some lists
    timestamps, opens, high, low, closed, volume, amount, dividend_timestamps = [], [], [], [], [], [], [], []
    # add them to a mock matrix for later use
    matrix = [timestamps, opens, high, low, closed, volume, amount, dividend_timestamps]

    split_ratio = 1
    for result in price_values['prices']:
        # print(result)
        if 'splitRatio' in result:
            '''For some reason the numerator and the denominator are flipped in yahoos api so that is why these seem backwards
            '''
            denominator = result[u'numerator']
            numerator = result[u'denominator']
            split_ratio = split_ratio * float(numerator) / float(denominator)
            # amount = fix_dividend_values(numer, denom, amount)
        elif 'amount' in result:
            # we pass this because the json file keeps track of dividends, may want to use this in this file
            # because this will tell us about dividiend stocks
            amount.append(result[u'amount'] / split_ratio)
            # this also comes as a unix timestamp
            dividend_timestamps.append(result[u'date'])
        else:
            # this comes as a unix timestamp
            timestamps.append(result[u'date'])
            opens.append(result[u'open'])
            high.append(result[u'high'])
            low.append(result[u'low'])
            closed.append(result[u'close'])
            volume.append(result[u'volume'])
            # adj_close.append(result[u'adjclose'])

    # we reverse the string because in google the dates go the other direction
    for i in range(len(matrix)):
        matrix[i].reverse()

    # unix timesteps are the number of seconds since 1970 something
    for i in range(len(timestamps)):
        # this converts them to a readable format
        timestamps[i] = dt.datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d')

    # same thing for this list as well
    for i in range(len(dividend_timestamps)):
        dividend_timestamps[i] = dt.datetime.fromtimestamp(dividend_timestamps[i]).strftime('%Y-%m-%d')

    stock_info = pd.DataFrame()
    divi_info = pd.DataFrame()
    stock_info['tradedate'] = timestamps
    stock_info['opened'] = opens
    stock_info['high'] = high
    stock_info['low'] = low
    stock_info['closed'] = closed
    stock_info['volume'] = volume
    # adjusted closed will be calculated later once we have the relevant dataframes and such
    # stock_info['adj_close'] = adj_close

    divi_info['tradedate'] = dividend_timestamps
    divi_info['diviamount'] = amount

    # this is to help with visualizing the data and also for
    # when I can figure out an RNN
    stock_info['timestep'] = list(range(0, len(volume)))

    data_df = stock_info

    divi_df = divi_info

    return data_df, divi_df


def yahoo_pricing(quote, date_range_week=True):
    """A function that is going to just return a giant dataframe of yahoo data for the last 5 years
    """

    data_df = pd.DataFrame()
    divi_df = pd.DataFrame()

    # call resolve_url to get the proper timestamps
    if date_range_week:
        request = requests.get(resolve_url(quote))
    else:
        request = requests.get(get_basic_url(quote))

    html = request.text

    soup = BeautifulSoup(html, 'lxml')

    testing = soup.find_all('script')

    for script in testing:
        temp = str(script)
        if '(function (root)' in temp:
            data_df, divi_df = parse_json(temp)
            # self.divi_df = temp_frame

    # new function to convert all the column names to uppercase for the sql
    data_df.columns = [col.upper() for col in data_df.columns]
    divi_df.columns = [col.upper() for col in divi_df.columns]

    return data_df, divi_df


def iex_pricing(stock):
    iex_values = requests.get(f"https://api.iextrading.com/1.0/stock/{stock}/chart/1d")

    iex_df = pd.DataFrame(iex_values.json())
    iex_df.columns = [c.upper() for c in iex_df.columns]
    iex_df = iex_df.rename(index=str, columns={'DATE': 'TRADEDATE'})
    iex_df['RECEIVETIME'] = pd.to_datetime(iex_df['TRADEDATE'] + iex_df["MINUTE"], format="%Y%m%d%H:%M")
    print(iex_df.dtypes)
    iex_df = iex_df[["TRADEDATE", "RECEIVETIME", "HIGH", "LOW", "AVERAGE", "VOLUME", "NOTIONAL", "NUMBEROFTRADES",
                     "MARKETHIGH", "MARKETLOW", "MARKETAVERAGE", "MARKETVOLUME", "MARKETNUMBEROFTRADES", "OPEN",
                     "CLOSE", "MARKETOPEN", "MARKETCLOSE", "CHANGEOVERTIME", "MARKETCHANGEOVERTIME"]]

    return iex_df


if __name__ == '__main__':
    pass
