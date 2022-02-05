from datetime import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Sequence,
    String,
    and_,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship

# postgres_host = "192.168.2.13"               # 数据库地址
postgres_host = "pg-quant-invest"  # 数据库地址
postgres_port = "5432"  # 数据库端口
postgres_user = "postgres"  # 数据库用户名
postgres_password = "password"  # 数据库密码
postgres_database = "market"  # 数据库名字
db_string = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_database}"

Base = declarative_base()


class Equity(Base):
    __tablename__ = "equity"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer, Sequence("equity_id_seq", schema="stock"), primary_key=True
    )
    sec_id = Column(String(255))  #     证券ID
    ticker = Column(String(255))  #     交易代码
    exchange_cd = Column(
        String(255)
    )  #     交易市场。例如，XSHG-上海证券交易所；XSHE-深圳证券交易所；XBEI-北京证券交易所。对应data_aPI.sys_code_get.code_type_id=10002。
    list_sector_cd = Column(Integer)  #     上市板块编码。1-主板；2-创业板；4-科创板；5-北交所。
    list_sector = Column(String(255))  #     上市板块
    trans_curr_cd = Column(
        String(255)
    )  #     交易货币。例如，CNY-人民币元；USD-美元。对应data_aPI.sys_code_get.code_type_id=10004。
    sec_short_name = Column(String(255))  #     证券简称
    sec_full_name = Column(String(255))  #     证券全称
    list_status_cd = Column(
        String(255)
    )  #     上市状态。L-上市；S-暂停；DE-终止上市；UN-未上市。对应data_aPI.sys_code_get.code_type_id=10005。
    list_date = Column(DateTime)  #     上市日期
    delist_date = Column(DateTime)  #     摘牌日期
    equ_type_cd = Column(String(255))  #     股票分类编码。例如，A-沪深A股；J-北证A股；B-沪深B股。
    equ_type = Column(String(255))  #     股票类别
    ex_country_cd = Column(
        String(255)
    )  #     交易市场所属地区。例如，CHN-中国大陆；HKG-香港。对应data_aPI.sys_code_get.code_type_id=10002。
    party_id = Column(Integer, default=0)  #     机构内部ID
    total_shares = Column(Float)  # t    总股本(最新)
    non_rest_float_shares = Column(Float)  # t    公司无限售流通股份合计(最新)
    non_rest_float_a = Column(
        Float
    )  # t    无限售流通股本(最新)。如果为A股，该列为最新无限售流通A股股本数量；如果为B股，该列为最新流通B股股本数量
    office_addr = Column(String(255))  #     办公地址
    prime_operating = Column(String(255))  #     主营业务范围
    end_date = Column(DateTime)  #     财务报告日期
    tsh_equity = Column(Float)  # t    所有者权益合计

    def __init__(self):
        pass


class TradeCalendar(Base):
    __tablename__ = "trade_calendar"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer,
        Sequence("trade_calendar_id_seq", schema="stock"),
        primary_key=True,
    )
    exchange_cd = Column(String(255))  #     证券交易所
    calendar_date = Column(DateTime)  #     日期
    is_open = Column(Boolean)  #     日期当天是否开市。0表示否，1表示是
    prev_trade_date = Column(DateTime)  #     当前日期前一交易日
    is_week_end = Column(Boolean)  #     当前日期是否当周最后交易日。0表示否，1表示是
    is_month_end = Column(Boolean)  #     当前日期是否当月最后交易日。0表示否，1表示是
    is_quarter_end = Column(Boolean)  #     当前日期是否当季最后交易日。0表示否，1表示是
    is_year_end = Column(Boolean)  #     当前日期是否当年最后交易日。0表示否，1表示是

    def __init__(self):
        pass


class MktIdxDay(Base):
    __tablename__ = "mkt_idx_day"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer,
        Sequence("mkt_idx_day_id_seq", schema="stock"),
        primary_key=True,
    )
    index_id = Column(String(255))
    ticker = Column(String(255))
    porg_full_name = Column(String(255))
    sec_short_name = Column(String(255))
    exchange_cd = Column(String(255))
    trade_date = Column(DateTime)
    pre_close_index = Column(Float)
    open_index = Column(Float)
    lowest_index = Column(Float)
    highest_index = Column(Float)
    closee_index = Column(Float)
    turnover_value = Column(Float)
    turnover_vol = Column(Float)
    chg = Column(Float)
    chg_pct = Column(Float)

    def __init__(self):
        pass


class MktEquDay(Base):
    __tablename__ = "mkt_equ_day"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer,
        Sequence("mkt_equ_day_id_seq", schema="stock"),
        primary_key=True,
    )
    sec_id = Column(String(255))  # 通联编制的证券编码，可使用DataAPI.SecIDGet获取
    ticker = Column(String(255))  #    通用交易代码
    sec_short_name = Column(String(255))  # ) secShortName; //    证券简称
    exchange_cd = Column(String(255))  # exchangeCD;    //    通联编制的交易市场编码
    trade_date = Column(DateTime)  # tradeDate;  //交易日期
    pre_close_price = Column(Float)  # preClosePrice;  //昨收盘(前复权)
    act_pre_close_price = Column(Float)  # actPreClosePrice;  //实际昨收盘价(未复权)
    open_price = Column(Float)  # openPrice;  //开盘价
    highest_price = Column(Float)  # highestPrice;  //最高价
    lowest_price = Column(Float)  # lowestPrice;  //最低价
    close_price = Column(Float)  # closePrice;  //收盘价
    turnover_vol = Column(Float)  # turnoverVol;  //成交量
    turnover_value = Column(Float)  # turnoverValue;  //成交金额，A股单位为元，B股单位为美元或港币
    deal_amount = Column(Float)  # dealAmount;  //    成交笔数
    turnover_rate = Column(Float)  # turnoverRate;  //日换手率，成交量/无限售流通股数
    accum_adj_bf_factor = Column(
        Float
    )  # accumAdjBfFactor;  //累积前复权因子，前复权价=未复权价*累积前复权因子。前复权是对历史行情进行调整，除权除息当日的行情无需调整。最近一次除权除息日至最新交易日期间的价格也无需调整，该期间前复权因子等于1。
    neg_market_value = Column(Float)  # negMarketValue;  //流通市值，收盘价*无限售流通股数
    market_value = Column(Float)  # marketValue;  //总市值，收盘价*总股本数
    chg_pct = Column(Float)  # chgPct;  //涨跌幅，收盘价/昨收盘价-1
    pe = Column(Float)  # PE;  //滚动市盈率，即市盈率TTM，总市值/归属于母公司所有者的净利润TTM
    pe1 = Column(Float)  # PE1;  //动态市盈率，总市值/归属于母公司所有者的净利润（最新一期财报年化）
    pb = Column(Float)  # PB;  //市净率，总市值/归属于母公司所有者权益合计
    is_open = Column(Integer)  # isOpen; //股票今日是否开盘标记：0-未开盘，1-交易日
    vwap = Column(Float)  # vwap;  //VWAP，成交金额/成交量
    accum_adj_af_factor = Column(Float)  # accumAdjAfFactor;)

    def __init__(self):
        pass


class FundDay(Base):
    __tablename__ = "fund_day"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer, Sequence("fund_day_id_seq", schema="stock"), primary_key=True
    )
    sec_id = Column(
        String(255)
    )  # secID; //    通联编制的证券编码，可使用DataAPI.SecIDGet获取
    ticker = Column(String(255))  # ticker; //    通用交易代码
    exchange_cd = Column(String(255))  # exchangeCD; //    通联编制的交易市场编码
    sec_short_name = Column(String(255))  # secShortName; //    证券简称
    trade_date = Column(DateTime)  # tradeDate; //    交易日期
    pre_close_price = Column(Float)  # preClosePrice;  //    昨收盘
    open_price = Column(Float)  # openPrice;  //    今开盘
    highest_price = Column(Float)  # highestPrice;  //    最高价
    lowest_price = Column(Float)  # lowestPrice;  //    最低价
    close_price = Column(Float)  # closePrice;  //    收盘价
    chg = Column(Float)  # CHG;  //    涨跌，收盘价-昨收盘价
    chg_pct = Column(Float)  # CHGPct;  //    涨跌幅，收盘价/昨收盘价-1
    turnover_vol = Column(Float)  # turnoverVol;  //    成交量
    turnover_value = Column(Float)  # turnoverValue;  //    成交金额
    discount = Column(Float)  # discount;  //    贴水，净值-收盘价
    discount_ratio = Column(Float)  # discountRatio;  //    贴水率，(净值-收盘)/净值
    circulation_shares = Column(Float)  # circulationShares;  //    流通份额
    accum_adj_factor = Column(Float)  # accumAdjFactor;  //    累积后复权因

    def __init__(self):
        pass


# 不完整字段
class Fund(Base):
    __tablename__ = "fund"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer, Sequence("fund_id_seq", schema="stock"), primary_key=True
    )
    sec_id = Column(
        String(255)
    )  # secID; //    通联编制的证券编码，可使用DataAPI.SecIDGet获取
    ticker = Column(String(255))  # ticker; //    通用交易代码
    sec_short_name = Column(String(255))  # secShortName; //    证券简称
    category = Column(String(255))
    operation_mode = Column(String(255))
    index_fund = Column(String(255))
    etf_lof = Column(String(255))
    is_qdii = Column(String(255))
    is_fof = Column(String(255))
    exchange_cd = Column(String(255))
    list_status_cd = Column(String(255))
    list_date = Column(DateTime)
    delist_date = Column(DateTime)
    idx_id = Column(String(255))
    idx_ticker = Column(String(255))
    idx_short_name = Column(String(255))

    def __init__(self):
        pass


class SyncStatus(Base):
    __tablename__ = "sync_status"
    __table_args__ = {"schema": "stock"}

    id = Column(
        Integer,
        Sequence("sync_status_id_seq", schema="stock"),
        primary_key=True,
    )
    rc = Column(Boolean)
    table_name = Column(String(64))  #     证券交易所
    update_time = Column(DateTime)  #     日期
    comment = Column(String(255))  # 更新说明

    def __init__(self):
        pass
