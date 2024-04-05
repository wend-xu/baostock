from sql_helper.bao_stock_helper import BaoStockHelper

start = "2024-03-07"
end = '2024-03-22'
(BaoStockHelper().conn().conn_bs()
 .sync_all_stock_k_day_to_temp(start_date=start, day=end)
 .cp_from_temp(start_date=start, day=end).dis_conn().dis_conn_bs())
