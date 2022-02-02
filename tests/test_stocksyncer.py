
# -*- coding: UTF-8 -*-
from syd.domain import MktIdxDay
import pytest
from syd.stocksyncer import *
from datetime import datetime, date

#测试equity表的更细情况

given = pytest.mark.parametrize
skipif = pytest.mark.skipif
skip = pytest.mark.skip
xfail = pytest.mark.xfail

def test_tus_code_split():
    assert tus_code_split('000001.SZ')[0] == '000001'
    assert tus_code_split('000001.SZ')[1] == 'XSHE'

    assert tus_code_split('000001.SH')[0] == '000001'
    assert tus_code_split('000001.SH')[1] == 'XSHG'

    assert tus_code_split('000001.BJ')[0] == '000001'
    assert tus_code_split('000001.BJ')[1] == 'XBEI'

def test_sector_cd_lambda():
    assert 1 ==(lambda x:{'主板':1, '创业版':2, '科创版':4, '北交所':5}[x])('主板')
    assert 5 ==(lambda x:{'主板':1, '创业版':2, '科创版':4, '北交所':5}[x])('北交所')

class TestStockSync:
    @pytest.fixture(scope='class')
    def syncer(self):
        logging.info("Setup for Class")
        syncer = StockSyncer(is_export_csv=True)
        return syncer 

    @pytest.fixture(scope='class')
    def db(self):
        db = DBAdaptor()
        return db

    def test_equity_has_been_sync_to_latest(self, syncer, db):
        df_incremental, export_csv_list =syncer.sync_equity()

        logging.info("exported csv:" + str(export_csv_list))

        df_tus = pd.read_csv(export_csv_list[0])
        df_db_bf = pd.read_csv(export_csv_list[1])

        assert df_incremental is not None, "update 不能返回None"
        df_incremental.to_csv(f"/tmp/df_incremental.csv")

        df_db_latest,csv_latest = db.getDfBySql("select ticker from stock.equity")
        assert df_db_latest['ticker'].is_unique, "不能有重复的股票"

        sizediff = df_db_latest.shape[0] - df_db_bf.shape[0]

        assert sizediff == df_incremental.shape[0], "增量更新应该等于原来的差值"
        df_db_sync, csv_latest = db.getDfBySql("select * from stock.sync_status where table_name= 'equity'")
        assert df_db_sync is not None
        assert df_db_sync.iloc[0]['update_time'].date() == date.today(), "sync_status已经被更新"

    def test_trade_calendar_has_been_sync_to_latest(self, syncer, db):
        df_incremental = syncer.sync_trade_calendar()
        assert df_incremental is not None, "update 不能返回None"



        df_db, db_bf_csv_file =  db.getDfBySql("select calendar_date from stock.trade_calendar order by trade_calendar desc limit 1")
        assert df_db is not None

        if df_incremental.shape[0] != 0:
            assert df_db.iloc[0]['calendar_date'] == df_incremental.index[-1], "最新的日期已经同步到数据库中"
        else:
            logging.info("trade_calendar已经更新到今年最后一天了")

        df_db_sync, csv_latest = db.getDfBySql("select * from stock.sync_status where table_name= 'trade_calendar'")
        assert df_db_sync is not None
        assert df_db_sync.iloc[0]['update_time'].date() == date.today(), "sync_status已经被更新"
        assert df_db_sync.iloc[0]['comment'] != "", "sync_status comment 字段不为空"

