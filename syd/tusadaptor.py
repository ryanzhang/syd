from datetime import datetime
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
    def __init__(self , is_use_cache=False, is_export_csv=False):
        ts.set_token('fcac0839e48a75577a0dd4db26a743d50fcfd4f80480cc720d76b6ac')
        self.conn = ts.pro_api()
        self.is_use_cache = is_use_cache
        self.is_export_csv = is_export_csv
        # logging.info("Current Profile:" + AppConfig.profile)


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
                logging.info("Loading Data from TUShare remotely::" )
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
                logging.info("DF Size:" + str(df.size) +", Cache file:" + str(df_cache_file))
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
                logging.info("Loading Data from TUShare remotely::" )
                df= self.conn.trade_cal(start_date=start_date.strftime("%Y%m%d"))
            except Exception as e:
                logging.error("loading data from tus failure" + traceback.format_exc()) 
                logging.error("exception is: " + str(e) )
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your api.")
            else:
                logging.info("DF Size:" + str(df.size) +", Cache file:" + str(df_cache_file))
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)

        if self.is_export_csv: 
            csv_file_path = AppConfig.cache_folder + "tus_stock_basic.csv"
            df.to_csv(csv_file_path)
        else:
            csv_file_path = None

        return df,csv_file_path
