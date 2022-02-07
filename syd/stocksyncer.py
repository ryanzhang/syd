# -*- coding: UTF-8 -*-
from datetime import datetime, timedelta

import pandas as pd

# from  pandas.tseries.offsets import MonthEnd, YearEnd
from kupy.config import configs
from kupy.dbadaptor import DBAdaptor
from kupy.logger import logger
from sqlalchemy.ext.declarative import declarative_base

from syd.domain import (
    Equity,
    Fund,
    FundDay,
    MktEquDay,
    SyncStatus,
    TradeCalendar,
)
from syd.tusadaptor import TUSAdaptor

Base = declarative_base()


class StockSyncer:
    def __init__(self, is_export_csv=False):
        self.tus = TUSAdaptor(is_export_csv=is_export_csv)
        self.db = DBAdaptor()
        self.calendar = self.db.get_df_by_sql(
            "select * from stock.trade_calendar"
        )
        pass

    def get_trade_calendar(self) -> pd.DataFrame:
        if self.calendar is None:
            self.calendar = self.db.get_df_by_sql(
                "select * from stock.trade_calendar"
            )
        return self.calendar

    def is_open_day(self, thedate) -> bool:
        ret = False
        df = self.get_trade_calendar()
        if (df["calendar_date"] == thedate).any():
            ret = df.loc[
                (df.calendar_date == thedate) & (df.exchange_cd == "XSHG"),
                "is_open",
            ].iloc[0]
        # logger.info(type(ret.iloc[0]))
        return ret

    # 返回
    # ret1: 增量股票代码 Series
    # ret2: Cache_file list [tablename_db_timestamp.csv, tablename_tus_timestamp.csv, tablename_timestamp.csv]
    def sync_equity(self) -> tuple[pd.Series, list]:
        df_tushare, tus_csv_file = self.tus.get_stock_basic_info()
        df_db, db_bf_csv_file = self.db.get_df_csv_by_sql(
            "select ticker from stock.equity"
        )
        df_incremental = df_tushare[~df_tushare.symbol.isin(df_db.ticker)]

        origin_size = df_db.shape[0]
        incr_size = 0
        if df_incremental is not None:
            incr_size = df_incremental.shape[0]
        entitylist = list()
        for index, row in df_incremental.iterrows():

            e = Equity()
            ticker, exchange_cd = TUSAdaptor.tus_code_split(row["ts_code"])
            e.sec_id = ticker + "." + exchange_cd
            e.ticker = ticker
            e.sec_short_name = row["name"]
            e.exchange_cd = exchange_cd
            e.list_sector = row["market"]
            e.list_sector_cd = (
                lambda x: {"主板": 1, "创业版": 2, "科创版": 4, "北交所": 5}[x]
            )(row["market"])
            e.list_status_cd = (lambda x: {"L": "L", "D": "DE", "P": "S"}[x])(
                row["list_status"]
            )
            e.list_date = datetime.strptime(row["list_date"], "%Y%m%d").date()
            e.delist_date = (
                None
                if row["delist_date"] is None
                else datetime.strptime(row["delist_date"], "%Y%m%d").date()
            )
            e.ex_country_cd = "CHN"
            entitylist.append(e)

        rc = self.db.save_all(entitylist)
        # Start to update update table
        if not self.db.update_any_by_id(
            SyncStatus,
            1,
            {
                "rc": rc,
                "update_time": datetime.now(),
                "comment": f"更新前:{origin_size},增量:{incr_size}",
            },
        ):
            logger.warning("Update sync_status table failed!")
        if not rc:
            export_filepath = (
                "/tmp/equity_sync_"
                + datetime.now().strftime("%Y%m%d%H%M%S")
                + ".csv"
            )
            df_incremental.to_csv(export_filepath)
            logger.error(
                "Save records to Stock.equity error, the incremental \
                records has been store to:"
                + export_filepath
            )
            raise Exception(
                "Store incremental data to DB Error, please correct it!"
            )
        logger.info(f"equity 表更新完毕, 更新前:{origin_size},增量:{incr_size}")
        return df_incremental, [tus_csv_file, db_bf_csv_file]

    # 返回
    # ret1: 增量股票代码 Series
    # ret2: Cache_file list [tablename_db_timestamp.csv, tablename_tus_timestamp.csv, tablename_timestamp.csv]
    def sync_trade_calendar(self) -> pd.DataFrame:
        df_db = self.db.get_df_by_sql(
            "select calendar_date from stock.trade_calendar order by trade_calendar desc"
        )
        if df_db is None or df_db.shape[0] == 0:
            raise Exception("数据库trade_calendar状态不对，请立即检查!")

        bf_latest_date = df_db.iloc[0]["calendar_date"]
        if bf_latest_date > datetime.today().date():
            logger.debug("trade_calendar已经是最新状态，无需更新..")
            return pd.DataFrame()
        start_date = bf_latest_date + timedelta(days=1)
        # 默认只取'SSE'上海交易所的交易日期，不再单独保存深圳交易所的交易日期
        df_tushare, tus_csv_file = self.tus.get_trade_cal(start_date)
        if df_tushare.shape[0] == 0:
            # Return empty DataFrame
            return pd.DataFrame()
        df_tushare["cal_date"] = pd.to_datetime(df_tushare["cal_date"])
        df_tushare["pretrade_date"] = pd.to_datetime(
            df_tushare["pretrade_date"]
        )
        df = df_tushare.set_index("cal_date")
        df_open = df.loc[df.is_open == 1, :].drop(
            columns=["is_open", "exchange", "pretrade_date"]
        )
        df_open["is_week_end"] = False
        df_open["is_month_end"] = False
        df_open["is_year_end"] = False

        df_open.loc[
            df_open.groupby(df_open.index.to_period("W")).apply(
                lambda x: x.index.max()
            ),
            "is_week_end",
        ] = True
        df_open.loc[
            df_open.groupby(df_open.index.to_period("M")).apply(
                lambda x: x.index.max()
            ),
            "is_month_end",
        ] = True
        df_open.loc[
            df_open.groupby(df_open.index.to_period("Y")).apply(
                lambda x: x.index.max()
            ),
            "is_year_end",
        ] = True

        df_transform = pd.merge(df, df_open, how="left", on="cal_date")
        df_transform.fillna(value=0, inplace=True)

        entitylist = list()
        for index, row in df_transform.iterrows():

            tc = TradeCalendar()
            tc.exchange_cd = (lambda x: {"SSE": "XSHG", "SZSE": "XSHE"}[x])(
                row["exchange"]
            )
            tc.calendar_date = index
            tc.is_open = bool(row["is_open"])
            tc.prev_trade_date = row["pretrade_date"]
            tc.is_week_end = row["is_week_end"]
            tc.is_month_end = row["is_month_end"]
            tc.is_year_end = row["is_year_end"]
            entitylist.append(tc)

        after_latest_date = df_tushare.iloc[-1]["cal_date"].date()
        rc = self.db.save_all(entitylist)
        # Start to update update table
        if not self.db.update_any_by_id(
            SyncStatus,
            9,
            {
                "rc": rc,
                "update_time": datetime.now(),
                "comment": f"更新前:{bf_latest_date},更新后:{after_latest_date}",
            },
        ):
            logger.warning("Update sync_status table failed!")
        if not rc:
            export_filepath = (
                configs["data_folder"].data
                + "cache/"
                + "equity_sync_"
                + datetime.now().strftime("%Y%m%d%H%M%S")
                + ".csv"
            )
            df_transform.to_csv(export_filepath)
            logger.error(
                "Save records to Stock.trade_calendar error, the incremental \
                records has been store to:"
                + export_filepath
            )
            raise Exception(
                "Store incremental data to DB Error, please correct it!"
            )

        return df_transform

    def get_latest_trade_date(self) -> datetime:
        # ll = datetime.today().strftime('%Y%m%d')
        today = datetime.today().date()
        df = self.get_trade_calendar()
        if (df["calendar_date"] == today).any():
            ret = df.loc[
                (df.calendar_date == today) & (df.exchange_cd == "XSHG"), :
            ]
            return (
                ret.iloc[0]["calendar_date"]
                if ret.iloc[0]["is_open"]
                else ret.iloc[0]["prev_trade_date"]
            )
        else:
            raise Exception("数据库不包含今天的数据，请立即修复!")

    # 返回
    # ret1: 增量股票代码 DataFrame对象
    def fetch_latest_mkt_equity_day_data(self) -> pd.DataFrame:
        latest_k_date = self.db.get_df_by_sql(
            "select trade_date from stock.mkt_equ_day order by trade_date desc limit 1"
        ).iloc[0]["trade_date"]

        latest_t_date = self.get_latest_trade_date()

        # start_date
        if latest_k_date > latest_t_date:
            raise Exception("数据库日线数据不应该有未来时数据")

        fetch_date = latest_k_date + timedelta(days=1)
        data_list = []
        if fetch_date > latest_t_date:
            logger.info(f"日线数据mkt_equ_day已经更新到{latest_k_date}")
            return pd.DataFrame()

        while fetch_date <= latest_t_date:
            # 跳过不开盘的日子,避免不必要的远程调用
            if self.is_open_day(fetch_date):
                df1 = self.tus.get_mkt_equd(fetch_date)
                df2 = self.tus.get_mt_equd_extra(fetch_date)
                df3 = self.tus.get_mkt_equdhfq(fetch_date)
                df = pd.merge(
                    df1, df2, how="left", on=["ts_code", "trade_date"]
                )
                df = pd.merge(
                    df, df3, how="left", on=["ts_code", "trade_date"]
                )
                data_list.append(df)
            fetch_date = fetch_date + timedelta(days=1)
        ret = pd.concat(data_list)
        return ret

    def sync_mkt_equ_d(self):
        df = self.fetch_latest_mkt_equity_day_data()
        if df is None or df.shape[0] == 0:
            logger.info("没有获取到更新数据，可能数据已经更新到最新了!")
        else:
            file_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
            if bool(configs["remote_api_cache_pkl"].data):
                df.to_pickle(
                    configs["data_folder"].data
                    + "cache/"
                    + "sync_mkt_equ_d_"
                    + file_suffix
                    + ".pkl"
                )
            if bool(configs["remote_api_cache_csv"].data):
                df.to_csv(
                    configs["data_folder"].data
                    + "cache/"
                    + "sync_mkt_equ_d_"
                    + file_suffix
                    + ".csv"
                )
            self.write_to_db(df)
            logger.info(f"mkt_equ_d表更新完毕, 增加记录{df.shape[0]}条!")

    # 获取日线表中没有的股票数据
    def get_missing_equ_day_data(self) -> pd.DataFrame:
        # 获取增量的股票数据
        df_expect = self.db.get_df_by_sql(
            "select sec_id, list_date from stock.equity where list_status_cd='L' "
        )
        df_actual = self.db.get_df_by_sql(
            "select distinct sec_id from stock.mkt_equ_day order by sec_id"
        )
        df_new = df_expect[~df_expect.sec_id.isin(df_actual.sec_id)]

        end_date = datetime.today().date()
        df_new = df_new[df_new["list_date"] <= end_date]
        logger.info(
            f"Found missing 股票列表: {df_new['sec_id'].astype(str).values.tolist()}"
        )
        logger.info("开始从tushare获取日线数据")

        df = self.tus.get_mkt_equd_by_codelist(
            sec_ids=df_new["sec_id"],
            start_date=df_new["list_date"].min(),
            end_date=end_date,
        )

        return df

    def write_to_db(self, df: pd.DataFrame) -> bool:
        if df is None or df.shape[0] == 0:
            logger.info("保存0条数据到数据库。")
            return False
        df_equ = self.db.get_df_by_sql(
            "select sec_id,ticker, sec_short_name from \
            stock.equity"
        )
        entitylist = list()
        for index, row in df.iterrows():
            ticker, exchange_cd = TUSAdaptor.tus_code_split(row["ts_code"])
            k_item = MktEquDay()
            k_item.sec_id = ticker + "." + exchange_cd
            k_item.ticker = ticker

            # sec_short_name
            df_sec_name = df_equ.loc[df_equ.ticker == ticker, "sec_short_name"]
            if df_sec_name is not None and df_sec_name.shape[0] == 1:
                k_item.sec_short_name = df_sec_name.iloc[0]
            else:
                logger.error(f"没有在equity表中找到{ticker}对应的股票名称, 请稍后核查!")

            k_item.exchange_cd = exchange_cd
            k_item.trade_date = row["trade_date"]
            k_item.pre_close_price = row["pre_close"]
            k_item.open_price = row["open"]
            k_item.highest_price = row["high"]
            k_item.lowest_price = row["low"]
            k_item.close_price = row["close_x"]
            k_item.turnover_vol = row["vol"] * 100
            k_item.turnover_value = row["amount"] * 1000
            # dealAmount 含义不详, 已忽略
            k_item.turnover_rate = row["turnover_rate_f"] / 100
            k_item.accum_adj_bf_factor = 1
            k_item.neg_market_value = row["circ_mv"] * 100000000
            k_item.market_value = row["total_mv"] * 100000000
            k_item.chg_pct = row["pct_chg"] / 100
            # 静态市盈率 k_item.pe 靜態市盈率=公司總市值/去年淨利潤
            k_item.pe = row["pe"]
            k_item.pe1 = row["pe_ttm"]
            k_item.pb = row["pb"]
            k_item.is_open = 0 if row["open"] == 0.0 else 1
            k_item.vwap = (
                k_item.turnover_value / k_item.turnover_vol
                if k_item.is_open
                else 0.0
            )
            k_item.accum_adj_af_factor = row["adj_factor"]
            entitylist.append(k_item)
        rc = self.db.save_all(entitylist)
        bf_latest_date = df.iloc[0]["trade_date"]
        after_latest_date = df.iloc[-1]["trade_date"]
        if not self.db.update_any_by_id(
            SyncStatus,
            7,
            {
                "rc": rc,
                "update_time": datetime.now(),
                "comment": f"更新前(最新未更新日期):{bf_latest_date},更新后(最后已更新日期):{after_latest_date}",
            },
        ):
            logger.warning("Update sync_status table failed!")

        return True

    def sync_fund_day(self):
        df = self.fetch_remote_fund_day_data()
        if df is None or df.shape[0] == 0:
            logger.info("没有获取到更新数据，可能数据已经更新到最新了!")
        else:
            file_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
            if bool(configs["remote_api_cache_pkl"].data):
                df.to_pickle(
                    configs["data_folder"].data
                    + "cache/"
                    + "sync_fund_day_"
                    + file_suffix
                    + ".pkl"
                )
            if bool(configs["remote_api_cache_csv"].data):
                df.to_csv(
                    configs["data_folder"].data
                    + "cache/"
                    + "sync_fund_day_"
                    + file_suffix
                    + ".csv"
                )
            self.write_fund_day_to_db(df)
            logger.info(f"fund_day表更新完毕, 增加记录{df.shape[0]}条!")

    def fetch_remote_fund_day_data(self) -> pd.DataFrame:
        latest_k_date = self.db.get_df_by_sql(
            "select max(trade_date) as trade_date \
            from stock.fund_day"
        ).iloc[0]["trade_date"]

        latest_t_date = self.get_latest_trade_date()

        # start_date
        if latest_k_date > latest_t_date:
            raise Exception("数据库日线数据不应该有未来时数据")

        fetch_date = latest_k_date + timedelta(days=1)
        data_list = []
        if fetch_date > latest_t_date:
            logger.info(f"日线数据mkt_equ_day已经更新到{latest_k_date}")
            return pd.DataFrame()

        while fetch_date <= latest_t_date:
            # 跳过不开盘的日子,避免不必要的远程调用
            if self.is_open_day(fetch_date):
                df1 = self.tus.get_fundd(fetch_date)
                df2 = self.tus.get_fundd_hfq(fetch_date)
                df = pd.merge(
                    df1, df2, how="left", on=["ts_code", "trade_date"]
                )
                data_list.append(df)
            fetch_date = fetch_date + timedelta(days=1)
        ret = pd.concat(data_list)
        return ret

    def write_fund_day_to_db(self, df: pd.DataFrame) -> bool:
        if df is None or df.shape[0] == 0:
            logger.info("保存0条数据到数据库。")
            return False
        df_fund = self.db.get_df_by_sql(
            "select distinct ticker, sec_short_name from \
            stock.fund where list_status_cd='L'"
        )
        df["ticker"] = df["ts_code"][0:6]
        df_outlier = df[~df.ticker.isin(df_fund.ticker)]
        update_dict = dict()
        for index, row in df_outlier.iterrows():
            update_dict[row["ticker"]] = {"list_status_cd": "L"}

        self.db.update_any_by_ticker(Fund, update_dict)

        df_fund = self.db.get_df_by_sql(
            "select distinct ticker, sec_short_name from \
            stock.fund where list_status_cd='L'"
        )

        entitylist = list()
        for index, row in df.iterrows():
            ticker, exchange_cd = TUSAdaptor.tus_code_split(row["ts_code"])
            k_item = FundDay()
            k_item.sec_id = ticker + "." + exchange_cd
            k_item.ticker = ticker

            # sec_short_name
            df_sec_name = df_fund.loc[
                df_fund.ticker == ticker, "sec_short_name"
            ]
            if df_sec_name is not None and df_sec_name.shape[0] == 1:
                k_item.sec_short_name = df_sec_name.iloc[0]
            else:
                logger.error(f"没有在fund表中找到{ticker}对应的股票名称, 请稍后核查!")

            k_item.exchange_cd = exchange_cd
            k_item.trade_date = row["trade_date"]
            k_item.pre_close_price = row["pre_close"]
            k_item.open_price = row["open"]
            k_item.highest_price = row["high"]
            k_item.lowest_price = row["low"]
            k_item.close_price = row["close"]
            k_item.turnover_vol = row["vol"] * 100
            k_item.turnover_value = row["amount"] * 1000
            k_item.chg_pct = row["pct_chg"]
            k_item.accum_adj_factor = row["adj_factor"]

            entitylist.append(k_item)
        rc = self.db.save_all(entitylist)
        bf_latest_date = df.iloc[0]["trade_date"]
        after_latest_date = df.iloc[-1]["trade_date"]
        if not self.db.update_any_by_id(
            SyncStatus,
            5,
            {
                "rc": rc,
                "update_time": datetime.now(),
                "comment": f"更新前(最新未更新日期):{bf_latest_date},更新后(最后已更新日期):{after_latest_date}",
            },
        ):
            logger.warning("Update sync_status table failed!")

        return True
