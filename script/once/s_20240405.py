import sys

from sql_helper.bao_stock_helper import BaoStockHelper
from index.macd import Macd
from index.ols import Ols
import numpy as np

code = "sz.000651"
bsh = BaoStockHelper().conn()
stock_data_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2023-01-01", day="2024-04-03")
stock_macd = Macd(k_data=stock_data_df)
dif_s_l = np.array([])
try:
    dif_s_l = stock_macd.get_dif_s_l()
except Exception as ex:
    print(f"\033[91m捕获到异常:{str(ex)} \033[0m")
    sys.exit()

ols = Ols(data=stock_data_df, ols_data_key="dif_12_26")
ols.upward_fitting(start_step=7)
# for i in range(12):
#     ols.ols(i, 7)
# ols.ols(0, 24)
# ols.plot()
print("")
