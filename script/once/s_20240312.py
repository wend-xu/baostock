from sql_helper.xq_stock_helper import XqStockHelper

# xqStockHelper = XqStockHelper()
# xqStockHelper.conn()
# symbol_list = ['SH603083', 'SZ002281', 'SH601138',
#                'SZ000651', 'SZ000333', 'SH600036',
#                'SH601012', 'SH600438', 'SZ000625', 'SZ000063', 'SH600160']
# for symbol in symbol_list:
#     last_10_day = xqStockHelper.last_x_day_index(symbol, 3)
#     print(
#         "{symbol}:{name} 共得到 {total} 条记录，涨幅是 {last_x_day_percent} ,上涨天数： {last_x_day_upper} ,涨幅方差：{last_x_day_var}".format(
#             **last_10_day))
# xqStockHelper.dis_conn()

code = "sh.603083"
print(code[:2].upper()+code[3:])
