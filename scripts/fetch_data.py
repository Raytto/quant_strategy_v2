from data_fetcher.tushare_sync_basic import data_sync as basic_sync
from data_fetcher.tushare_sync_daily import sync as daily_sync
from data_fetcher.vacuum import db_vacuum

if __name__ == "__main__":
    basic_sync()
    daily_sync()
    db_vacuum()
