import datetime
import pytest
from syd.dbadaptor import DBAdaptor
import os
import logging

from syd.config import AppConfig

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

given = pytest.mark.parametrize
skipif = pytest.mark.skipif
skip = pytest.mark.skip
xfail = pytest.mark.xfail

logging.info("Start Testing")

query_sql = "select * from stock.equity"
expect_cache_file_path = AppConfig.cache_folder + DBAdaptor.calculateCacheFilename(query_sql) + ".pkl"
class TestDBAdaptor:
    @pytest.fixture(scope='class')
    def db(self):
        logging.info("Setup for Class")
        print("Setup for Class")
        db = DBAdaptor(is_use_cache=True)
        if os.path.exists(expect_cache_file_path):
            os.remove(expect_cache_file_path)
        return db

    def test_cache_file_name(self,db):
        cache1 = db.calculateCacheFilename("select * from stock.equity")
        
        assert 5 ==  len(cache1)
        cache2 = db.calculateCacheFilename("select * from stock.fund")
        assert 5 ==  len(cache2)
        assert cache1 != cache2

    def test_get_equity_without_cache(self,db):
        db.setCacheMode(False)
        df,csv_file = db.getDfAndCsvBySql(query_sql)
        assert df is not None
        assert csv_file is not None
        assert not os.path.exists(expect_cache_file_path)
        assert os.path.exists(csv_file)

    def test_get_equity_with_cache(self,db):
        db.setCacheMode(True)
        df,csv_file = db.getDfAndCsvBySql(query_sql)
        assert df is not None
        assert csv_file is not None
        assert os.path.exists(expect_cache_file_path)
        assert os.path.exists(csv_file)

    def test_update_sync_status(self,db):
        # db.setCacheMode(True)
        assert db.updateSyncStatus("equity", True, datetime.datetime.now(), "更新前:55,增量:10")

    @skip
    # Caution when you delete your data
    def test_no_duplicate(self, db):
        df_mkt_idx_d = db.getDfBySql("select * from stock.mkt_idx_day")
        df_mkt_idx_d.sort_values(['index_id', 'trade_date'], inplace=True)

        df_dup = df_mkt_idx_d[df_mkt_idx_d.duplicated(['trade_date','index_id'])]
        assert 0 ==df_dup.shape[0]
        # for index in df_dup.id:
        #     db.deleteById(MktIdxDay, index)
