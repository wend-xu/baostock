from collections import defaultdict
from datetime import datetime

import mysql.connector

from entity.bs_stock_data_day_k import BsStockDataDayK
from entity.xq_stock_data_day import XqStockDataDay
from sql_helper.bao_stock_helper import BaoStockHelper

x_day = 5
code = 'sz.002463'
bao_stock_helper = BaoStockHelper()
bao_stock_helper.conn()
# last_x_day_k = bao_stock_helper.get_stock_last_x_day_k(code=code, last_x_day=x_day)
# percent_sum = sum(day_one['pctChg'] for day_one in last_x_day_k)
# print(percent_sum)
#
# all_stock_last_10_day_percent = bao_stock_helper.get_all_stock_last_x_day_percent(7)
# all_stock_last_10_day_percent.sort(key=lambda day_one: 0 if day_one['percent'] is None else day_one['percent'],
#                                    reverse=True)
#
# last_10_day_more_then_15_percent = [one for one in all_stock_last_10_day_percent if
#                                     one['percent'] is not None and one['percent'] >= 30]
# print("过去 10 个工作日涨幅超过10%的股票数： {count}".format(count=len(last_10_day_more_then_15_percent)))
# group_last_10_day_more_then_10_percent = {}
# for item in last_10_day_more_then_15_percent:
#     industry_list = group_last_10_day_more_then_10_percent.get(item['industry'])
#     if industry_list is None:
#         industry_list_init = [item]
#         group_last_10_day_more_then_10_percent[item['industry']] = industry_list_init
#     else:
#         industry_list.append(item)
group_last_10_day_more_then_10_percent = bao_stock_helper.group_by_industry_and_ge_percent(last_x_day=10,least_percent=30)
print("过去 10 个工作日涨幅超过10%的股票数 行业统计： ")
for industry in group_last_10_day_more_then_10_percent:
    industry_data = group_last_10_day_more_then_10_percent[industry]
    print(f"行业：{industry} 数量：{len(industry_data)}")
    for industry_data_one in industry_data:
        print(industry_data_one)
bao_stock_helper.dis_conn()
