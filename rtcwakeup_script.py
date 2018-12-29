'''
Created on Dec 15, 2018

Premise of this module is to run the rtcwakeup command and call this module from cron instead of figureing out the date portion in bash

@author: jack
'''
import os
import datetime as dt
import time
import sys

def main():
    now = dt.datetime.now()
    
    with open('/home/jack/timecheck.txt', 'a') as cron_checker:
        cron_checker.write('Good evening the time is {} the crontab worked!!!'.format(now))
    
    now = now + dt.timedelta(days=1)
    now = now.replace(hour=6, minute=0, second=0, microsecond=0)

    os.system("""sudo rtcwake -m no -l -t "$(date -d '{}' '+%s')" """.format(now))
    
    time.sleep(5)
    
    os.system("""sudo poweroff""")

if __name__ == '__main__':
    sys.exit(main())
