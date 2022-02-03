from datetime import datetime, timedelta
import time
import tushare as ts

import pandas as pd
import os
import logging
import sys
import traceback

from syd.config import AppConfig


logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

class TUSAdaptor:
    ts_code_mapper = {'SZ':'XSHE', 'SH':'XSHG', 'BJ':'XBEI'}
    db_code_mapper = {'XSHE':'SZ', 'XSHG':'SH', 'XBEI':'BJ'}
    def __init__(self , is_use_cache=False, is_export_csv=False):
        # token 1: 1860
        ts.set_token('2840885933ecda6d41176bd967e549b590044f3ee8a910214b23ad72')
        #token 2: 1861
        # ts.set_token('a7cdb32dce95207bb05e8fce6eb9124377c599508490e4538535f609')
        #token 3: 1851
        # ts.set_token('7fd86d168266ec8740bd13eb4f9f9b157b7c0c2a5609a8cfc4e5bdaa')
        self.conn = ts.pro_api(timeout=60)
        self.is_use_cache = is_use_cache
        self.is_export_csv = is_export_csv

    # return tuple(str,str):
    # list[0]:ticker
    # list[1]: exchange_cd
    @staticmethod
    def tus_code_split(ts_code) ->tuple[str,str]:
        mapper={'SZ':'XSHE', 'SH':'XSHG', 'BJ':'XBEI'}
        return ts_code[0:6], mapper[ts_code[7:9]]
    
    @staticmethod
    def ts_code_to_sec_id(ts_code)->str:
        return ts_code[0:6]+"."+TUSAdaptor.ts_code_mapper[ts_code[7:9]]


    def setCacheMode(self,is_use_cache):
        self.is_use_cache = is_use_cache

    def getStockBasicInfo(self) -> tuple[pd.DataFrame, str]:
        if self.is_use_cache:
            df_cache_file = AppConfig.cache_folder + "tus_stock_basic.pkl"
        else:
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                df= self.conn.stock_basic(fields='ts_code,symbol,name,area,\
                    industry,fullname,market,exchange,list_status,list_date,\
                    delist_date,is_hs' )
            except Exception as e:
                logging.error("loading data from tus failure" + traceback.format_exc()) 
                logging.error("exception is: " + str(e) )
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your api.")
            else:
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)

        if self.is_export_csv: 
            csv_file_path = AppConfig.cache_folder + "tus_stock_basic.csv"
            df.to_csv(csv_file_path)
        else:
            csv_file_path = None

        return df,csv_file_path

    #start_date -> datetime
    def getTradeCal(self, start_date:datetime) -> tuple[pd.DataFrame, str]:
        if self.is_use_cache:
            df_cache_file = AppConfig.cache_folder + "tus_trade_cal.pkl"
        else:
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                logging.debug("Loading Data from TUShare remotely::" )
                df= self.conn.trade_cal(start_date=start_date.strftime("%Y%m%d"))
            except Exception as e:
                logging.error("loading data from tus failure" + traceback.format_exc()) 
                logging.error("exception is: " + str(e) )
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your api.")
            else:
                logging.debug("DF Size:" + str(df.size) +", Cache file:" + str(df_cache_file))
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)

        if self.is_export_csv: 
            csv_file_path = AppConfig.cache_folder + "tus_stock_basic.csv"
            df.to_csv(csv_file_path)
        else:
            csv_file_path = None

        return df,csv_file_path

    #获取一天的交易数据
    #一天一个账户不能超过50次
    def getMktEquD(self, trade_date:datetime, ts_code="" ):

        df = pd.DataFrame()
        # 允许重试3次
        for count in range(3):
            try:
                df= self.conn.daily(ts_code=ts_code, trade_date=trade_date.strftime('%Y%m%d'))
            except:
                logging.info(f"{trade_date} 日期数据 重试{count}次...")
                if count == 2:
                    raise Exception("重试3次仍然失败，终止运行!")
                time.sleep(2)
            else:
                break
        # df.sort_values(['ts_code', 'trade_date'], inplace=True)
        return df

    def getMktEquDExtra(self, trade_date:datetime, ts_code=""):
        df = pd.DataFrame()
        # 允许重试3次
        for count in range(3):
            try:
                df= self.conn.daily_basic(ts_code=ts_code, \
                    trade_date=trade_date.strftime('%Y%m%d'))
            except:
                logging.info(f"{trade_date} 日期数据 重试{count}次...")
                if count == 2:
                    raise Exception("重试3次仍然失败，终止运行!")
                time.sleep(2)
            else:
                break
        # df.sort_values(['ts_code', 'trade_date'], inplace=True)
        return df

    def getMktEquDhfq(self, trade_date:datetime, ts_code=""):
        df = pd.DataFrame()
        # 允许重试3次
        for count in range(3):
            try:
                df= self.conn.adj_factor(ts_code=ts_code, trade_date=trade_date.strftime('%Y%m%d') )
            except:
                logging.info(f"{trade_date} 日期数据 重试{count}次...")
                if count == 2:
                    raise Exception("重试3次仍然失败，终止运行!")
                time.sleep(2)
            else:
                break
        # df.sort_values(['ts_code', 'trade_date'], inplace=True)
        return df

    #根据股票列表取回一天的数据
    #code_list不能超过1000
    def getMktEquDByCodeList(self, sec_ids:pd.Series, start_date:datetime, \
        end_date:datetime )->pd.DataFrame:
        ts_code_str_list = ""
        for index, sec_id in sec_ids.items():
            ts_code_str_list= ts_code_str_list + sec_id[0:6]+"." + TUSAdaptor.db_code_mapper[sec_id[7:11]] + ","
            # ts_code_str_list= ts_code_str_list + sec_id + ","
        
        if ts_code_str_list == "":
            raise Exception  ("code_list不能为空!")

        logging.info(f"Start to fetch {ts_code_str_list} during {start_date} {end_date} from tus ")
        df = pd.DataFrame()
        # 允许重试3次
        for count in range(3):
            try:
                df1= self.conn.daily(ts_code=ts_code_str_list, start_date=start_date.strftime('%Y%m%d'), end_date=end_date.strftime('%Y%m%d') )
                time.sleep(1)
                df2= self.conn.daily_basic(ts_code=ts_code_str_list, \
                    start_date=start_date.strftime('%Y%m%d'), \
                        end_date=end_date.strftime('%Y%m%d'))
                time.sleep(1)
                df3= self.conn.adj_factor(ts_code=ts_code_str_list, \
                    start_date=start_date.strftime('%Y%m%d'), \
                        end_date=end_date.strftime('%Y%m%d') )
                time.sleep(1)
                df = pd.merge(df1, df2, how="left", on=["ts_code", "trade_date"])
                df = pd.merge(df, df3, how="left", on=["ts_code", "trade_date"])
            except Exception as e:
                logging.info(f"重试{count}次... Exception:{e}")
                time.sleep(1)
            else:
                break
        return df

    #后复权行情 从老接口获取, 直接调用ts 而不是pro_api
    def getMktEquDAdjAf(self, code_list, start_date):
        df = pd.DataFrame()
        for code in code_list:
            ts_code = code[0:6] + "." + TUSAdaptor.db_code_mapper[code[7:11]]
            df_item = ts.pro_bar(ts_code=ts_code, adj='hfq', \
                start_date=start_date.strftime('%Y%m%d'))
                   
            df = pd.concat([df, df_item])
        
        df.sort_values(['ts_code', 'trade_date'], inplace=True)
        return df

