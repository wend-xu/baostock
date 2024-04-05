from decimal import Decimal

import numpy as np

from sql_helper.bao_stock_index_helper import BaoStockIndexHelper
from sql_helper.bao_stock_helper import BaoStockHelper
import pandas as pd
from sklearn.metrics import r2_score

bsh = BaoStockHelper().conn()
bsih = BaoStockIndexHelper().conn()

### 计算过去120天 个股与沪深 300的跟踪程度
### 获取过去 120 个交易日的指数k线数据
### 获取与指数时间范围相同的个股k线数据
### 如果二者数量不匹配，执行数据过滤，将二者按日期对应，如果对应日期不是二者届存在，丢弃该点
### 使用 pd 对处理后的数据计算 皮尔逊相关系数

sh0003000index120day = bsih.get_index_last_x_day_as_df(index_code="sh.000300", x=120)
last_date = sh0003000index120day['date'][0]
first_date = sh0003000index120day['date'].iloc[-1]
all_stock = bsh.get_all_stock_with_date(last_date)
pearson_correlation_list = []
for code in all_stock['code']:
    print(f"股票代码：{code} ,开始计算 皮尔逊相关系数")
    stock_data_in_range = bsh.get_stock_date_in_date_range_as_df(code=code, start_date=first_date, day=last_date)
    stock_data_in_range = stock_data_in_range[stock_data_in_range['pctChg'].notnull()]
    # 找到两个DataFrame中共有的日期
    common_dates = sh0003000index120day['date'].isin(stock_data_in_range['date']) & stock_data_in_range['date'].isin(
        sh0003000index120day['date'])
    # 使用common_dates来过滤每个DataFrame
    sh0003000index120day_filtered = sh0003000index120day[common_dates]
    stock_data_in_range_filtered = stock_data_in_range[stock_data_in_range['date'].isin(sh0003000index120day['date'])]
    try:
        pearson_correlation = stock_data_in_range_filtered['pctChg'].corr(sh0003000index120day_filtered['pctChg'])
        diff = stock_data_in_range_filtered['pctChg']-sh0003000index120day_filtered['pctChg']
        diff = diff.apply(lambda x: float(x))
        std = np.std(diff)
        r_squared = r2_score(sh0003000index120day_filtered['pctChg'], stock_data_in_range_filtered['pctChg'])
        print(f"皮尔逊相关系数: {pearson_correlation} ,跟踪误差 ：{std} ,r_squared: {r_squared} ")
        pearson_correlation_list.append(
             {"code": code, "pearson_correlation_120": pearson_correlation, "std": std, "r_squared": r_squared})
    except Exception:
        print("捕获到异常")
print("计算完成")
pd.DataFrame(pearson_correlation_list).to_csv(f"{first_date}_{last_date}_pearson_correlation_120.csv")
