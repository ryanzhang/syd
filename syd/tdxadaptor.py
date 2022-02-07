import logging
import os

import pandas as pd
from mootdx.quotes import Quotes

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

stock_cache_file = "sz_stock_cache.pkl"

if os.path.isfile(stock_cache_file):
    df_load = pd.read_pickle(stock_cache_file)
else:
    try:
        client = Quotes.factory(market="std")
        df_load = client.stocks(0)

    except Exception:
        logging.error(" load data from tdx failure !")
        exit()

    if df_load.shape[0] == 0:
        logging.warn(" there is no data in !")
    else:
        # ts = time.time()
        # logging.info(" Load data from DB, takes time: " + str(ts-start_time))
        # 默认排序
        df_load = df_load.sort_values(by=["ticker"]).drop_duplicates()
        df_load.to_pickle(stock_cache_file)
        # logging.info(" Cache df to file, takes time:-" + str(time.time()-ts))

df_load.to_csv("sz_stock.csv")
# df_stock = df_load[df_load['code'].str.startswith('6')]
# df_stock.to_csv("6_.csv")

df_stock = df_load[df_load["code"].str.startswith("00")]
df_stock.to_csv("00_.csv")

df_stock = df_load[df_load["code"].str.startswith("300")]
df_stock.to_csv("300_.csv")
