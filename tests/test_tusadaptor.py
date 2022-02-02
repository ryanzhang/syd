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

expect_cache_filepath=AppConfig.cache_folder + "tus_stock_basic.pkl"
class TestTUSAdaptor:
    @pytest.fixture(scope='class')
    def tus(self):
        logging.info("Setup for Class")
        tus = TUSAdaptor(is_use_cache=True, is_export_csv=True)

        return tus

    def test_get_stock_basic_with_cache(self,tus):
        df,csv_filepath = tus.getStockBasicInfo()
        assert df is not None and df.size >0, "返回的df不能为空"
        assert csv_filepath is not None
        assert expect_cache_filepath is not None
        assert os.path.exists(expect_cache_filepath)
        assert os.path.exists(csv_filepath)

    @skip
    def test_get_stock_basic_without_cache(self,tus):
        #默认是没有cache的
        tus.setCacheMode(False)
        df,csv_filepath = tus.getStockBasicInfo()
        assert df is not None
        assert csv_filepath is None
        

if __name__ == '__main__':
    pytest.main(args=['-s', os.path.abspath(__file__)])
