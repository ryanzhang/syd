
# -*- coding: UTF-8 -*-
import pytest
from syd.sync_equity import update 

#测试equity表的更细情况
from syd.domain import *
given = pytest.mark.parametrize
skipif = pytest.mark.skipif
skip = pytest.mark.skip
xfail = pytest.mark.xfail

def test_equity_has_been_sync_to_latest():
    

def test_update_sync_status():
    # select top 1 trade_date from mkt_equ_day order by id desc
    assert True

def test_sync_trade_calendar():
    sync_trade_calendar()
    assert True
    # trade_calendar表中的最新日期，应该等于sync_status表中的最新数据
    # sync_status中的最新更新数据应该等于最后一个交易日的数据
