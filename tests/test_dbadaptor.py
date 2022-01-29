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

class TestDBAdaptor:
    @pytest.fixture(scope='class')
    def db(self):
        logging.info("Setup for Class")
        return DBAdaptor()

    def test_cache_file_name(self,db):
        cache1 = db.getCacheFilename("select * from stock.equity")
        assert 9 ==  len(cache1)
        cache2 = db.getCacheFilename("select * from stock.fund")
        assert 9 ==  len(cache2)
        assert cache1 != cache2

    def test_get_equity_with_cache(self,db):
        AppConfig.profile = "DEV"
        query_sql = "select * from stock.equity"
        df = db.getAllBySQL(query_sql)
        assert df is not None
        assert os.path.exists(db.getCacheFilename(query_sql))

    def test_get_equity_without_cache(self,db):
        AppConfig.profile = "PROD"
        query_sql = "select * from stock.equity"
        df = db.getAllBySQL(query_sql)
        assert df is not None
        assert ~os.path.exists(db.getCacheFilename(query_sql))

