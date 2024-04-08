from datetime import datetime, date
import sys

import pandas as pd

from sql_helper.bao_stock_helper import BaoStockHelper
from index.macd import Macd
from index.ols import Ols
import numpy as np

start = datetime.now()
# code = "sz.000651"
bsh = BaoStockHelper().conn()
all_in_date = bsh.get_all_stock_with_date("2024-04-03")
result_array = []
all_code = all_in_date['code']
for code in all_code:
    base_stock_data = bsh.get_base_stock_date(code)
    stock_data_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2022-01-01", day="2024-04-03")
pd.DataFrame(result_array).to_csv("upward_fitting_result.csv")
end = datetime.now()
print(f"总计耗时:{end - start}")
