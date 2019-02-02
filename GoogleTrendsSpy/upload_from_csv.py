"""

"""
import sys
from argparse import ArgumentParser
import pandas as pd
from CommWithDatabase import HandleDB
import datetime as dt


def append_to_db(dates):
    db = HandleDB()
    for day in dates:
        print(day)
        try:
            df = pd.read_csv('/home/jack/logging/google_trends_{}.csv'.format(day), index_col=0)
            #db.delete_from_table('SPYKEYWORDS_test', day)
            db.append_to_database(df, 'SPYKEYWORDS')
        except Exception as e:
            print(e)


def gen_dates(start_date):
    start = dt.datetime.strptime(start_date, '%Y-%m-%d').date()
    end = dt.date.today() + dt.timedelta(days=1)

    days = []
    while start != end:
        days.append(start.strftime('%Y-%m-%d'))
        start = start + dt.timedelta(days=1)

    return days


def main():
    parser = ArgumentParser()
    parser.add_argument('--dates', help='the days to append to the database')
    parser.add_argument('--run_all_start_date', help='try to run for all dates, accepts the starting date')
    args = parser.parse_args()
    if args.run_all_start_date is None:
        dates = args.dates.split(',')
    else:
        dates = gen_dates(args.run_all_start_date)
    append_to_db(dates)


if __name__ == '__main__':
    sys.exit(main())
