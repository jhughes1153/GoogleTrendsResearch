import sys
import requests
from sqlalchemy import create_engine
import datetime as dt
import os
import argparse


def send_message(failed, reasons):
    if failed:
        # red box will be posted with this one
        post_params = f'{{"bot_id": "ac960a6f08227f9b603e8d8859", "text": "{reasons}", "attachments": ' \
            f'[{{"type": "image", "url": "https://i.groupme.com/600x446.png.44de5706526a41a0ae3038e7714bbcce"}}]}}'
    else:
        # green box will post with this one
        post_params = '{"text": "Task failed successfully", "bot_id": "ac960a6f08227f9b603e8d8859", ' \
                      '"attachments": [{"type": "image", "url": ' \
                      '"https://i.groupme.com/472x270.png.9682c71f841c4a878be541cd8cf7cccf"}]}'

    response = requests.post('https://api.groupme.com/v3/bots/post', data=post_params)
    print(response)


def check_db_insert():
    cnxn = create_engine("mysql+pymysql://root:#1Runner!!@localhost:3306/SpyInformation")
    max_date = cnxn.execute("SELECT MAX(date_values) FROM SpyInformation.SPYKEYWORDS").fetchall()[0][0]

    today = dt.date.today()

    if today == max_date.date():
        return False
    else:
        return True


def check_day():
    if dt.date.today().weekday() in range(1, 6):
        return True
    else:
        return False


def check_iex_pricing():
    cnxn = create_engine("mysql+pymysql://root:#1Runner!!@localhost:3306/SpyInformation")
    max_date = cnxn.execute("SELECT MAX(TRADEDATE) FROM SpyInformation.SPYPRICING_IEX").fetchall()[0][0]

    today = dt.date.today() - dt.timedelta(days=1)

    if today == max_date:
        return False
    else:
        return True


def check_csv_exists(csv_path):
    today = dt.date.today().strftime('%Y-%m-%d')
    csv_final = f'{csv_path}/google_trends_{today}.csv'
    if os.path.isfile(csv_final):
        return False
    else:
        return True


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--csv_path', help='path to the csv file to check that it was generated')
    args = parser.parse_args()

    failed = False
    reasons = []

    failed_db = check_db_insert()
    failed_path = check_csv_exists(args.csv_path)
    if check_day():
        if check_iex_pricing():
            failed = True
            reasons.append("No Iex values for today")
    else:
        print('Skipping iex check for today as the market was not open and it did not run')

    if failed_db:
        failed = True
        reasons.append('Max tradedate and current tradedate do not match')

    if failed_path:
        failed = True
        reasons.append('Csvs not generated today')

    send_message(failed, reasons)


if __name__ == "__main__":
    sys.exit(main())
