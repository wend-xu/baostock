import datetime

from sql_helper.bao_stock_helper import BaoStockHelper
from sql_helper.bao_stock_index_helper import BaoStockIndexHelper

day = "2024-04-03"
# 同步当日的k线数据 每日执行
# 同步当日个股k线数据
BaoStockHelper().conn().conn_bs().sync_all_stock_k_day_to_temp(day=day).cp_from_temp(
    day=day).clear_k_data_temp().dis_conn().dis_conn_bs()
# 同步当日的指数k线数据
index_code_list = ['sh.000001', 'sz.399006', 'sz.399106', 'sh.000300']
(BaoStockIndexHelper().conn().conn_bs().clear_temp_index_data().sync_index_k_day_to_temp(
    index_code_list=index_code_list, day=day)
 .cp_index_k_day_from_temp(index_code_list=index_code_list, day=day).clear_temp_index_data().dis_conn().dis_conn_bs())
