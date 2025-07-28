from data_fetcher.tushare_sync_basic import data_sync as basic_sync
from data_fetcher.tushare_sync_daily_a import data_sync as daily_a_sync
from data_fetcher.tushare_sync_daily_h import data_sync as daily_h_sync
from data_fetcher.vacuum import db_vacuum

if __name__ == "__main__":
    basic_sync()
    daily_h_sync()
    daily_a_sync()
    db_vacuum()
