from datetime import datetime
from sqlalchemy import and_
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session

Base = declarative_base()

class Equity(Base):
    __tablename__ = 'equity'

    sec_id = Column(String(255))  # 	证券ID
    ticker = Column(String(255))  # 	交易代码
    exchange_cd = Column(String(255))  # 	交易市场。例如，XSHG-上海证券交易所；XSHE-深圳证券交易所；XBEI-北京证券交易所。对应data_aPI.sys_code_get.code_type_id=10002。
    list_sector_cd = Column(Integer) # 	上市板块编码。1-主板；2-创业板；4-科创板；5-北交所。
    list_sector = Column(String(255))  # 	上市板块
    trans_curr_cd = Column(String(255))  # 	交易货币。例如，CNY-人民币元；USD-美元。对应data_aPI.sys_code_get.code_type_id=10004。
    sec_short_name = Column(String(255))  # 	证券简称
    sec_full_name = Column(String(255))  # 	证券全称
    list_status_cd = Column(String(255))  # 	上市状态。L-上市；S-暂停；DE-终止上市；UN-未上市。对应data_aPI.sys_code_get.code_type_id=10005。
    list_date = Column(DateTime) # 	上市日期
    delist_date = Column(DateTime) # 	摘牌日期
    equ_type_cd = Column(String(255))  # 	股票分类编码。例如，A-沪深A股；J-北证A股；B-沪深B股。
    equ_type = Column(String(255))  # 	股票类别
    ex_country_cd = Column(String(255))  # 	交易市场所属地区。例如，CHN-中国大陆；HKG-香港。对应data_aPI.sys_code_get.code_type_id=10002。
    party_id = Column(Integer) # 	机构内部ID
    total_shares = Column(Float) # t	总股本(最新)
    nonrest_float_shares = Column(Float) # t	公司无限售流通股份合计(最新)
    nonrestfloat_a = Column(Float) # t	无限售流通股本(最新)。如果为A股，该列为最新无限售流通A股股本数量；如果为B股，该列为最新流通B股股本数量
    office_addr = Column(String(255))  # 	办公地址
    prime_operating = Column(String(255))  # 	主营业务范围
    end_date = Column(DateTime) # 	财务报告日期
    tsh_equity = Column(Float) # t	所有者权益合计


class TradeCalendar(Base):
    __tablename__ = 'trade_calendar'

    exchange_cd = Column(String(255)) # 	证券交易所
    calendar_date = Column(DateTime)  # 	日期
    is_open = Column(Boolean)  # 	日期当天是否开市。0表示否，1表示是
    prev_trade_date = Column(DateTime)  # 	当前日期前一交易日
    is_week_end = Column(Boolean)  # 	当前日期是否当周最后交易日。0表示否，1表示是
    is_month_end = Column(Boolean)  # 	当前日期是否当月最后交易日。0表示否，1表示是
    is_quarter_end = Column(Boolean)  # 	当前日期是否当季最后交易日。0表示否，1表示是
    is_year_end = Column(Boolean)  # 	当前日期是否当年最后交易日。0表示否，1表示是


#更新同步表的数据信息
def update_sync_status():

# 
def sync_trade_calendar():
    return True
