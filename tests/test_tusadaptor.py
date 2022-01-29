import pytest
from syd.tusadaptor import TUSAdaptor
import os
import logging

from syd.config import AppConfig

logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s - %(levelname)s- %(message)s"
)

given = pytest.mark.parametrize
skipif = pytest.mark.skipif
skip = pytest.mark.skip
xfail = pytest.mark.xfail

class TestTUSAdaptor:
    @pytest.fixture(scope='class')
    def tus(self):
        print("Setup for Class")
        logging.info("Setup for Class")
        return TUSAdaptor()

    def test_get_equity_with_cache(self,tus):
        AppConfig.profile = "DEV"
        df,cache_file = tus.getStockBasicInfo()
        assert df is not None
        assert cache_file is not None
        assert os.path.exists(cache_file)

    # @skip
    def test_get_equity_without_cache(self,tus):
        AppConfig.profile = "PROD"
        df,cache_file = tus.getStockBasicInfo()
        assert df is not None
        assert cache_file is None

if __name__ == '__main__':
    pytest.main(args=['-s', os.path.abspath(__file__)])
