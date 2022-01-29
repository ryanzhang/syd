
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
    update()
    


