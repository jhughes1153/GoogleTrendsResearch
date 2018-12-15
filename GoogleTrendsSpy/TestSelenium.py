'''
Created on Oct 27, 2018

@author: Jack
'''

import pandas as pd

from CommWithDatabase import HandleDB

a = HandleDB()

date_current = a.get_most_recent_date()

print(date_current)