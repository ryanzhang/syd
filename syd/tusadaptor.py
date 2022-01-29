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
    def __init__(self):
        ts.set_token('fcac0839e48a75577a0dd4db26a743d50fcfd4f80480cc720d76b6ac')
        self.conn = ts.pro_api()
        print("In TUSadaptor init method:" + str(type(self.conn)))
        self.is_use_cache = False
        logging.info("Current Profile:" + AppConfig.profile)

    def getStockBasicInfo(self) -> tuple[pd.DataFrame, str]:
        if 'DEV' == AppConfig.profile:
            self.is_use_cache = True
            df_cache_file = "tua_stock_basic" +".pkl"
        else:
            self.is_use_cache = False
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                logging.info("Execute Query:" )
                df= self.conn.stock_basic(fields='ts_code,symbol,name,area,\
                    industry,fullname,market,exchange,list_status,list_date,\
                    delist_date,is_hs' )
            except Exception as e:
                logging.error("loading data from tus failure" + traceback.format_exc()) 
                logging.error("exception is: " + str(e) + e.with_traceback()) 
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your api.")
            else:
                logging.info("DF Size:" + str(df.size) +", Cache file:" + str(df_cache_file))
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)
        return df,df_cache_file
