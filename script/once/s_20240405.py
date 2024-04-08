from datetime import datetime, date
import sys

import pandas as pd

from sql_helper.bao_stock_helper import BaoStockHelper
from index.macd import Macd
from index.ols import Ols
import numpy as np

start = datetime.now()
bsh = BaoStockHelper().conn()
# code = "sz.000651"
code = "sz.300738"
base_stock_data = bsh.get_base_stock_date(code)
stock_data_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2022-01-01", day="2024-04-03")
stock_macd = Macd(k_data=stock_data_df)
dif_s_l = np.array([])
try:
    dif_s_l = stock_macd.get_dif_s_l()
    ols = Ols(data=stock_data_df, ols_data_key="dif_12_26")
    upward_fitting_result = ols.upward_fitting(start_step=7)
    ols.ols(0,85).plot()
except Exception as ex:
    print(f"\033[91m捕获到异常:{str(ex)} \033[0m")
end = datetime.now()
bsh.dis_conn()
print(f"总计耗时:{end - start}")
