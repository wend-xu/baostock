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
ipoDateMax = date(2023, 1, 1)
for code in all_code:
    base_stock_data = bsh.get_base_stock_date(code)
    if base_stock_data.get('ipoDate') is None or base_stock_data['ipoDate'] > ipoDateMax:
        print("忽略2023年后上市个股")
        continue
    stock_data_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2022-01-01", day="2024-04-03")
    stock_macd = Macd(k_data=stock_data_df)
    dif_s_l = np.array([])
    try:
        dif_s_l = stock_macd.get_dif_s_l()
        ols = Ols(data=stock_data_df, ols_data_key="dif_12_26")
        upward_fitting_result = ols.upward_fitting(start_step=7)
        upper_cut_date = stock_data_df.iloc[upward_fitting_result['start']:upward_fitting_result['end']]['date']
        startDate = upper_cut_date.iloc[-1]
        endDate = upper_cut_date.iloc[0]
        result_array.append({'startDate':startDate,'endDate':endDate,**base_stock_data, **upward_fitting_result})
    except Exception as ex:
        print(f"\033[91m捕获到异常:{str(ex)} \033[0m")
        continue
pd.DataFrame(result_array).to_csv("upward_fitting_result.csv")
end = datetime.now()
print(f"总计耗时:{end - start}")
