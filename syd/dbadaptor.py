# -*- coding: UTF-8 -*-

# 提供缓存能力，避免反复从远端拉数据库
import pandas as pd
from datetime import datetime, timedelta

import psycopg2
import time, os.path
import sys
import logging
import hashlib

from syd.config import AppConfig

pd.set_option('expand_frame_repr', False)
logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s- %(message)s')

#导入数据
# postgres config
postgres_host = "pg-quant-invest"               # 数据库地址
postgres_port = "5432"       # 数据库端口
postgres_user = "user"              # 数据库用户名
postgres_password = "password"      # 数据库密码
postgres_datebase = "market"      # 数据库名字

_conn_string = "host=" + postgres_host + " port=" + postgres_port + " dbname=" + postgres_datebase + \
                " user=" + postgres_user + " password=" + postgres_password


class DBAdaptor:
    def __init__(self, conn_string):
        self.conn_string = conn_string
        self.conn = psycopg2.connect(self.conn_string)
        self.is_use_cache = False
        logging.info("Current Profile:" + AppConfig.profile)

    def __init__(self):
        self.conn_string = _conn_string
        self.conn = psycopg2.connect(self.conn_string)
        self.is_use_cache = False
        logging.info("Current Profile:" + AppConfig.profile)

    def getAllBySQL(self,  query_sql) -> pd.DataFrame:
        if 'DEV' == AppConfig.profile:
            self.is_use_cache = True
            df_cache_file = self.getCacheFilename(query_sql)
        else:
            self.is_use_cache = False
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                logging.info("Execute Query:" + query_sql)
                df= pd.read_sql(query_sql, self.conn)            
            except:
                logging.error("loading data from db failure")
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your query:" + query_sql)
            else:
                logging.info("DF Size:" + str(df.size) +", Cache file:" + str(df_cache_file))
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)
        return df
    
    def getCacheFilename(self, query_sql) -> str:
        return hashlib.md5(query_sql.encode()).hexdigest()[0:5]+ ".pkl"
