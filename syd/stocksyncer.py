# -*- coding: UTF-8 -*-
import logging
from syd.domain import Equity, SyncStatus, TradeCalendar
import pandas as pd;
from syd.dbadaptor import DBAdaptor
from syd.tusadaptor import TUSAdaptor
from datetime import datetime,timedelta
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session
# from  pandas.tseries.offsets import MonthEnd, YearEnd

Base = declarative_base()

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

# return tuple(str,str):
# list[0]:ticker
# list[1]: exchange_cd
def tus_code_split(ts_code) ->tuple[str,str]:
    mapper={'SZ':'XSHE', 'SH':'XSHG', 'BJ':'XBEI'}

    return ts_code[0:6], mapper[ts_code[7:9]]

class StockSyncer:
    def __init__(self, is_export_csv=False):
        self.tus = TUSAdaptor(is_export_csv=is_export_csv)
        self.db = DBAdaptor(is_export_csv=is_export_csv)
        pass

    #返回
    # ret1: 增量股票代码 Series
    # ret2: Cache_file list [tablename_db_timestamp.csv, tablename_tus_timestamp.csv, tablename_timestamp.csv]
    def sync_equity(self) -> tuple[pd.Series, list]:
        df_tushare, tus_csv_file  = self.tus.getStockBasicInfo()
        df_db, db_bf_csv_file =  self.db.getDfBySql("select ticker from stock.equity")
        df_incremental = df_tushare[~df_tushare.symbol.isin(df_db.ticker)]

        origin_size = df_db.shape[0]
        incr_size = 0
        if df_incremental is not None:
            incr_size = df_incremental.shape[0]
        entitylist = list()
        for index,row in df_incremental.iterrows():
            
            e = Equity()
            ticker,exchange_cd = tus_code_split(row['ts_code'])
            e.sec_id = ticker + "." + exchange_cd
            e.ticker = ticker
            e.sec_short_name = row['name']
            e.exchange_cd = exchange_cd
            e.list_sector = row['market']
            e.list_sector_cd = (lambda x:{'主板':1, '创业版':2, '科创版':4, '北交所':5}[x])(row['market'])
            e.list_date = datetime.strptime(row['list_date'], '%Y%m%d').date()
            e.delist_date = None if row['delist_date'] is None else datetime.strptime(row['delist_date'],'%Y%m%d').date() 
            e.ex_country_cd = 'CHN'
            entitylist.append(e)

        rc = self.db.saveAll(entitylist)
        #Start to update update table
        if not self.db.updateSyncStatus("equity",rc,datetime.now(), f"更新前:{origin_size},增量:{incr_size}"):
            logging.warning("Update sync_status table failed!")
        if not rc:
            export_filepath = "/tmp/equity_sync_" + datetime.now().strftime("%Y%m%d%H%M%S") +".csv"
            df_incremental.to_csv(export_filepath)
            logging.error("Save records to Stock.equity error, the incremental \
                records has been store to:" + export_filepath)
            raise Exception("Store incremental data to DB Error, please correct it!")

        return df_incremental, [tus_csv_file, db_bf_csv_file]

    #返回
    # ret1: 增量股票代码 Series
    # ret2: Cache_file list [tablename_db_timestamp.csv, tablename_tus_timestamp.csv, tablename_timestamp.csv]
    def sync_trade_calendar(self) -> pd.DataFrame:
        df_db, db_bf_csv_file =  self.db.getDfBySql("select calendar_date from stock.trade_calendar order by trade_calendar desc")
        if df_db is None or df_db.shape[0] == 0:
            raise Exception ("数据库状态不对，请立即检查!")

        bf_latest_date = df_db.iloc[0]['calendar_date'] 
        start_date = bf_latest_date + timedelta(days=1)
        #默认只取'SSE'上海交易所的交易日期，不再单独保存深圳交易所的交易日期
        df_tushare, tus_csv_file  = self.tus.getTradeCal(start_date)
        if df_tushare.shape[0] == 0:
            #Return empty DataFrame
            return pd.DataFrame()
        df_tushare['cal_date'] = pd.to_datetime(df_tushare['cal_date'])
        df_tushare['pretrade_date'] = pd.to_datetime(df_tushare['pretrade_date'])
        df = df_tushare.set_index('cal_date')
        df_open = df.loc[df.is_open == 1,:].drop(columns=['is_open', 'exchange', 'pretrade_date'])
        df_open['is_week_end'] = False
        df_open['is_month_end'] = False
        df_open['is_year_end'] = False

        df_open.loc[df_open.groupby(df_open.index.to_period('W')).apply(lambda x: x.index.max()),'is_week_end'] = True
        df_open.loc[df_open.groupby(df_open.index.to_period('M')).apply(lambda x: x.index.max()), 'is_month_end'] = True
        df_open.loc[df_open.groupby(df_open.index.to_period('Y')).apply(lambda x: x.index.max()), 'is_year_end'] = True

        df_transform = pd.merge(df, df_open, how='left', on='cal_date')
        df_transform.fillna(value=0, inplace=True)
        
        entitylist = list()
        for index,row in df_transform.iterrows():
            
            tc = TradeCalendar()
            tc.exchange_cd = (lambda x: {'SSE':'XSHG', 'SZSE':'XSHE'}[x])(row['exchange'])
            tc.calendar_date = index
            tc.is_open = bool(row['is_open'])
            tc.prev_trade_date = row['pretrade_date']
            tc.is_week_end = row['is_week_end']
            tc.is_month_end = row['is_month_end']
            tc.is_year_end = row['is_year_end']
            entitylist.append(tc)

        after_latest_date = df_tushare.iloc[-1]['cal_date'].date()
        rc = self.db.saveAll(entitylist)
        #Start to update update table
        if not self.db.updateSyncStatus("trade_calendar",rc,datetime.now(), f"更新前:{bf_latest_date},更新后:{after_latest_date}"):
            logging.warning("Update sync_status table failed!")
        if not rc:
            export_filepath = "/tmp/equity_sync_" + datetime.now().strftime("%Y%m%d%H%M%S") +".csv"
            df_transform.to_csv(export_filepath)
            logging.error("Save records to Stock.trade_calendar error, the incremental \
                records has been store to:" + export_filepath)
            raise Exception("Store incremental data to DB Error, please correct it!")

        return df_transform

    def sync_mkt_equity_d(self) ->:
