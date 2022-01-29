# -*- coding: UTF-8 -*-
import logging
import pandas as pd;
from syd.dbadaptor import DBAdaptor
from syd.tusadaptor import TUSAdaptor

PROFILE='DEV'

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

def fetch_from_tushare():
    tus = TUSAdaptor()
    df = tus.getStockBasic()
    return df

def fetch_from_db():
    db =DBAdaptor()
    df = db.getEquity()
    return df

def update():
    # 已经从TUShare 同步到本地
    df_db = fetch_from_db()
    df_tushare = fetch_from_tushare()
    df_incremental = df_tushare[~df_tushare.symbol.isin(df_db.ticker))

    df_incremental.to_csv("stock_basic_diff.csv")
    logging.info("已经从TUShare 同步到本地")

