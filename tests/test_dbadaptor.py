import datetime
from syd.domain import Equity, Fund, SyncStatus
import pytest
from kupy.dbadaptor import DBAdaptor
from kupy.logger import logger
from kupy.config import configs
import os


given = pytest.mark.parametrize
skipif = pytest.mark.skipif
skip = pytest.mark.skip
xfail = pytest.mark.xfail


query_sql = "select * from stock.equity"
expect_cache_file_path = (
    configs["data_folder"].data
    + "cache/"
    + DBAdaptor.get_hash_filename(query_sql)
    + ".pkl"
)


class TestDBAdaptor:
    @pytest.fixture(scope="class")
    def db(self):
        logger.info("Setup for Class")
        db = DBAdaptor(is_use_cache=True)
        if os.path.exists(expect_cache_file_path):
            os.remove(expect_cache_file_path)
        return db

    def test_cache_file_name(self, db):
        cache1 = db.get_hash_filename("select * from stock.equity")

        assert 5 == len(cache1)
        cache2 = db.get_hash_filename("select * from stock.fund")
        assert 5 == len(cache2)
        assert cache1 != cache2

    def test_get_equity_without_cache(self, db):
        db.set_cache_mode(False)
        df, csv_file = db.get_df_csv_by_sql(query_sql)
        assert df is not None
        assert csv_file is not None
        assert not os.path.exists(expect_cache_file_path)
        assert os.path.exists(csv_file)

    def test_get_equity_with_cache(self, db):
        db.set_cache_mode(True)
        df, csv_file = db.get_df_csv_by_sql(query_sql)
        assert df is not None
        assert csv_file is not None
        assert os.path.exists(expect_cache_file_path)
        assert os.path.exists(csv_file)

    def test_update_sync_status(self, db: DBAdaptor):
        # db.set_cache_mode(True)
        assert db.update_any_by_id(
            SyncStatus,
            1,
            {
                "rc": True,
                "update_time": datetime.datetime.now(),
                "comment": "更新前:55,增量:10",
            },
        )

    @skip
    # Caution when you delete your data
    def test_no_duplicate(self, db):
        df_mkt_idx_d = db.get_df_by_sql("select * from stock.mkt_idx_day")
        df_mkt_idx_d.sort_values(["index_id", "trade_date"], inplace=True)

        df_dup = df_mkt_idx_d[
            df_mkt_idx_d.duplicated(["trade_date", "index_id"])
        ]
        assert 0 == df_dup.shape[0]
        # for index in df_dup.id:
        #     db.deleteById(MktIdxDay, index)

    def test_load_equity(self, db):
        df_equ = db.get_df_by_sql(
            "select sec_id,ticker, sec_short_name from \
            stock.equity"
        )
        assert df_equ is not None and df_equ.shape[0] > 0
        logger.info(f"stock account:{df_equ.shape[0]}")
        ticker = "301217"
        assert (
            df_equ.loc[df_equ.ticker == ticker, "sec_short_name"].iloc[0]
            == "铜冠铜箔"
        )

    def test_update_any_by_ticker(self):
        update_dict = {
            "501216": {"list_status_cd": "L"},
            "513300": {"list_status_cd": "L"},
            "517200": {"list_status_cd": "L"},
        }
        db = DBAdaptor()
        db.update_any_by_ticker(Fund, update_dict)
        df = db.get_df_by_sql(
            "select ticker, list_status_cd from stock.fund where ticker in ('501216', '513300', '517200') "
        )
        logger.info(str(df))
        assert df.loc[df["list_status_cd"] != "L", :].shape[0] == 0

    def test_get_any_by_id(self, db: DBAdaptor):
        ss = db.get_any_by_id(SyncStatus, 1)
        assert ss is not None
        assert ss.table_name == "equity"
        assert ss.rc
