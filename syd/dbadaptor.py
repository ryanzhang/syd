# -*- coding: UTF-8 -*-

# 提供缓存能力，避免反复从远端拉数据库
import hashlib
import os.path
import time
import traceback
from datetime import datetime, timedelta
from typing import Any

import pandas as pd
import psycopg2
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    and_,
    create_engine,
    select,
    update,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship

from syd.domain import SyncStatus

Base = declarative_base()

from syd.config import configs
from syd.logger import logger

# pd.set_option('expand_frame_repr', False)

postgres_host = configs["postgres_host"].data
postgres_port = configs["postgres_port"].data
postgres_user = configs["postgres_user"].data
postgres_password = configs["postgres_password"].data
postgres_database = configs["postgres_database"].data

_conn_string = (
    "host="
    + postgres_host
    + " port="
    + postgres_port
    + " dbname="
    + postgres_database
    + " user="
    + postgres_user
    + " password="
    + postgres_password
)

_db_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"


class DBAdaptor:
    def __init__(self, conn_string="", is_use_cache=False):
        if conn_string == "":
            conn_string = _conn_string
        self.conn_string = conn_string
        self.conn = psycopg2.connect(self.conn_string)
        self.is_use_cache = is_use_cache
        self.engine = create_engine(_db_string)

    def setCacheMode(self, is_use_cache):
        self.is_use_cache = is_use_cache

    def getDfAndCsvBySql(self, query_sql) -> tuple[pd.DataFrame, str]:
        df = self.getDfBySql(query_sql)
        csv_file_path = (
            configs["cache_folder"].data
            + self.calculateCacheFilename(query_sql)
            + ".csv"
        )
        df.to_csv(csv_file_path)

        return df, csv_file_path

    def getDfBySql(self, query_sql) -> pd.DataFrame:
        if self.is_use_cache:
            df_cache_file = (
                configs["cache_folder"].data
                + self.calculateCacheFilename(query_sql)
                + ".pkl"
            )
        else:
            df_cache_file = None

        if self.is_use_cache and os.path.exists(df_cache_file):
            df = pd.read_pickle(df_cache_file)
        else:
            try:
                logger.debug(
                    f"Loading Query from pg_host:{postgres_host}, query_sql: {query_sql}"
                )
                df = pd.read_sql(query_sql, self.conn)
            except Exception as e:
                logger.error(
                    "loading data from db failure" + traceback.format_exc()
                )
                logger.error("Exception is: " + str(e))
                df = None
            if df is None or df.shape[0] == 0:
                logger.warning(
                    "there is no data,pls check your query:" + query_sql
                )
            else:
                if self.is_use_cache:
                    logger.debug(
                        f"DF Size: {df.size}  Cache file: {df_cache_file} is_use_cache: {self.is_use_cache}"
                    )
                    df.to_pickle(df_cache_file)

        return df

    def getAnyById(self, cls, id):
        session = Session(self.engine)
        return session.query(cls).filter(
            cls.id == id
        )

    def save(self, entity) -> bool:
        try:
            session = Session(self.engine)
            session.add(entity)
            session.commit()
        except Exception as e:
            logger.error("Save record to db error" + traceback.format_exc())
            logger.error("Exception is: " + str(e))
            return False

        return True

    def saveAll(self, entitylist) -> bool:
        try:
            session = Session(self.engine)
            session.add_all(entitylist)
            session.commit()
        except Exception as e:
            logger.error("Save record to db error" + traceback.format_exc())
            logger.error("Exception is: " + str(e))
            return False

        return True

    def updateSyncStatus(self, tablename, rc, update_time, comment):
        try:
            session = Session(self.engine)
            session.execute(
                update(SyncStatus)
                .where(SyncStatus.table_name == tablename)
                .values(rc=rc, update_time=update_time, comment=comment)
            )
            session.commit()
        except Exception as e:
            logger.error("Save record to db error" + traceback.format_exc())
            logger.error("Exception is: " + str(e))
            return False
        return True

    def updateAnyeByTicker(self, cls, update_dict: dict) -> bool:
        try:
            session = Session(self.engine)
            for key, value in update_dict.items():
                result = session.query(cls).filter(cls.ticker == key)
                if result is None:
                    logger.warning(f"{key} 不存在于{cls}表中，请检查！")
                    continue
                result.update(value)

            session.commit()
        except Exception as e:
            logger.error(
                "Update record to db {cls} error" + traceback.format_exc()
            )
            logger.error("Exception is: " + str(e))
            return False
        return True

    def deleteById(self, cls, id) -> bool:
        try:
            session = Session(self.engine)
            session.query(cls).filter(cls.id == id).delete()
            session.commit()
        except Exception as e:
            logger.error(
                "Delete record from db error" + traceback.format_exc()
            )
            logger.error("Exception is: " + str(e))
            return False

        return True

    @staticmethod
    def calculateCacheFilename(query_sql) -> str:
        return hashlib.md5(query_sql.encode()).hexdigest()[0:5]
