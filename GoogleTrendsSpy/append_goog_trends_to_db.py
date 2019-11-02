from argparse import ArgumentParser
from logger import get_logger
from data_collector import google_trends
import os
import datetime as dt
import pandas as pd
import glob
import getpass
from alerting import get_alerter
from CommWithDatabase import HandleDB
from common import log_on_failure

__app__ = f"google_trends_{getpass.getuser()}"
logger = get_logger('google_trends_analysis', __app__)
alerter = get_alerter()


def get_version(path: str, file_name: str) -> int:
    files = glob.glob(f'{path}/{file_name}.*')
    logger.info(files)
    if len(files) == 0:
        return 0
    else:
        return max([int(v.split('.')[-1]) for v in files]) + 1


def make_symlink(df: pd.DataFrame, path: str, sym_name: str, sep: str) -> None:
    today = dt.datetime.today().strftime('%Y_%m_%d')
    creation_file = f'{sym_name}_{today}.csv'
    logger.info(f'Generating file with name: {creation_file}')
    version = get_version(path, creation_file)

    true_path = f'{path}/{creation_file}.{version}'
    logger.info(f'Writing out csv to {true_path}')
    df.to_csv(true_path, sep=sep)

    symlink_path = f'{path}/{sym_name}.csv'
    if os.path.exists(symlink_path):
        logger.info(f"Unlinking {symlink_path}")
        os.unlink(symlink_path)
    os.symlink(true_path, symlink_path)
    alerter.info(f"updated symlink for {today}")


def main_impl(args):

    keywords = args.keywords.split(",")
    logger.info(keywords)
    logger.info(args.gecko_path)
    df_goog = google_trends(keywords, args.gecko_path)
    logger.info(f"Google trends dataframe shape: {df_goog.shape}")

    make_symlink(df_goog, args.path, __app__, args.sep)

    if not args.just_file:
        db = HandleDB()
        db.append_to_database(df_goog, args.table)
        logger.info(f"Appended df into {args.table}")
        alerter.info(f"Appended df into {args.table}")


@log_on_failure
def main():
    gecko_path = f"{os.path.dirname(os.path.abspath(__file__))}/extras/geckodriver_21"

    parser = ArgumentParser(description="New controller script to manage google trends stuff")
    parser.add_argument("--keywords", help="words to scrape from google trends",
                        default="Bitcoin,Altcoin,Ethereum,BTC,ETH,Crypto,Cryptocurrency,Currency")
    parser.add_argument("--table", help="table to insert into", required=True)
    parser.add_argument("--path", help="path to keep the csv", required=True)
    parser.add_argument("--sep", help="seperator", default="|")
    parser.add_argument("--gecko-path", help="geckodriver path to use default is _23",
                        default=gecko_path)
    parser.add_argument("--just-file", help="generate just the file", action="store_true")
    args = parser.parse_args()

    main_impl(args)

    alerter.send_message(__app__)


if __name__ == '__main__':
    main()
