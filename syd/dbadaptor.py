# -*- coding: UTF-8 -*-

# 提供缓存能力，避免反复从远端拉数据库
from syd.domain import SyncStatus
from typing import Any
import pandas as pd
from datetime import datetime, timedelta

import psycopg2
import time, os.path
import traceback
import logging
import hashlib
from datetime import datetime
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import select, update
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session

Base = declarative_base()

from syd.config import AppConfig

pd.set_option('expand_frame_repr', False)
logging.basicConfig(level=logging.INFO, format=' %(asctime)s - %(levelname)s- %(message)s')

#导入数据
# postgres config
postgres_host = "pg-quant-invest"               # 数据库地址
# postgres_host = "192.168.2.20"               # 数据库地址
postgres_port = "5432"       # 数据库端口
postgres_user = "user"              # 数据库用户名
postgres_password = "password"      # 数据库密码
postgres_datebase = "market"      # 数据库名字

_conn_string = "host=" + postgres_host + " port=" + postgres_port + " dbname=" + postgres_datebase + \
                " user=" + postgres_user + " password=" + postgres_password

_db_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_datebase}"

class DBAdaptor:
    def __init__(self, conn_string="", is_use_cache=False, is_export_csv=False):
        if conn_string == "":
           conn_string = _conn_string
        self.conn_string = conn_string
        self.conn = psycopg2.connect(self.conn_string)
        self.is_use_cache = is_use_cache
        self.is_export_csv = is_export_csv
        self.engine = create_engine(_db_string)
        # logging.info("Current Profile:" + AppConfig.profile)

    def setCacheMode(self,is_use_cache):
        self.is_use_cache = is_use_cache

    def getDfBySql(self,  query_sql) -> tuple[pd.DataFrame, str]:
        if self.is_use_cache:
            df_cache_file = AppConfig.cache_folder + self.calculateCacheFilename(query_sql) + ".pkl"
        else:
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                logging.info(f"Loading Query from pg_host:{postgres_host}, query_sql: {query_sql}")
                df= pd.read_sql(query_sql, self.conn)            
            except Exception as e:
                logging.error("loading data from db failure" + traceback.format_exc())
                logging.error("Exception is: " + str(e) + e.with_traceback()) 
                df = None     
            if df is None or df.shape[0]== 0:
                logging.warning("there is no data,pls check your query:" + query_sql)
            else:
                logging.info(f"DF Size: {df.size}  Cache file: {df_cache_file} is_use_cache: {self.is_use_cache}")
                if self.is_use_cache:
                    df.to_pickle(df_cache_file)

        if self.is_export_csv: 
            csv_file_path = AppConfig.cache_folder + self.calculateCacheFilename(query_sql) + ".csv"
            df.to_csv(csv_file_path)
        else:
            csv_file_path = None

        return df, csv_file_path

    def save(self, entity)->bool:
        try:
            session = Session(self.engine)
            session.add(entity)
            session.commit()
        except Exception as e:
            logging.error("Save record to db error" + traceback.format_exc())
            logging.error("Exception is: " + str(e))
            return False

        return True

    def saveAll(self, entitylist)->bool:
        try:
            session = Session(self.engine)
            session.add_all(entitylist);
            session.commit()
        except Exception as e:
            logging.error("Save record to db error" + traceback.format_exc())
            logging.error("Exception is: " + str(e) ) 
            return False

        return True

    def updateSyncStatus(self, tablename, rc, update_time, comment):
        try:
            session = Session(self.engine)
            session.execute(update(SyncStatus).where(SyncStatus.table_name == tablename).values(rc=rc,update_time=update_time, comment=comment))
            session.commit()
        except Exception as e:
            logging.error("Save record to db error" + traceback.format_exc())
            logging.error("Exception is: " + str(e) ) 
            return False
        return True

    # def updateById(self,entity)-> bool:
    #     try:
    #         session = Session(self.engine)
    #         session.add(entity)
    #         session.commit()
    #     except Exception as e:
    #         logging.error("Save record to db error" + traceback.format_exc())
    #         logging.error("Exception is: " + str(e) + e.with_traceback()) 
    #         return False
    #     return True

    def deleteById(self, cls, id)->bool:
        try:
            session = Session(self.engine)
            session.query(cls).filter(cls.id == id).delete()
            session.commit()
        except Exception as e:
            logging.error("Delete record from db error" + traceback.format_exc())
            logging.error("Exception is: " + str(e) ) 
            return False
        
        return True

    @staticmethod 
    def calculateCacheFilename(query_sql) -> str:
        return hashlib.md5(query_sql.encode()).hexdigest()[0:5]
