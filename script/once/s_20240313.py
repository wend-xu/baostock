from sql_helper.xq_stock_helper import XqStockHelper


def print_data(data_list):
    for last_x_day in data_list:
        print(
            "{symbol}:{name} 共得到 {total} 条记录，涨幅是 {last_x_day_percent} ,上涨天数： {last_x_day_upper} ,涨幅方差：{last_x_day_var} ，最新市值：{float_market_capital} 亿".format(
                **last_x_day))


xqStockHelper = XqStockHelper()
xqStockHelper.conn()
x_day = 7
top_x = 20
stack_symbol_all = xqStockHelper.analyse_all_last_x_day(last_x_day=x_day, top_x=top_x, end_date='2024-03-14')
percent_analyse = stack_symbol_all['percent_analyse']
all_top_x = percent_analyse[f'all_top_{top_x}']
gt_10_billion_top_x = percent_analyse[f'gt_10_billion_top_{top_x}']
le_10_billion_top_x = percent_analyse[f'le_10_billion_top_{top_x}']
print(f"\n\n过去{x_day}个工作日涨幅最高的{top_x}只个股：")
print_data(all_top_x)
print(f"\n\n过去{x_day}个工作日涨幅最高的{top_x}只个百亿股：")
print_data(gt_10_billion_top_x)
print(f"\n\n过去{x_day}个工作日涨幅最高的{top_x}只个微盘股：")
print_data(le_10_billion_top_x)

all_last_x = percent_analyse[f'all_last_{top_x}']
gt_10_billion_last_x = percent_analyse[f'gt_10_billion_last_{top_x}']
le_10_billion_last_x = percent_analyse[f'le_10_billion_last_{top_x}']
print(f"\n\n过去{x_day}个工作日跌幅最高的{top_x}只个股：")
print_data(all_last_x)
print(f"\n\n过去{x_day}个工作日跌幅最高的{top_x}只个百亿股：")
print_data(gt_10_billion_last_x)
print(f"\n\n过去{x_day}个工作日跌幅最高的{top_x}只个微盘股：")
print_data(le_10_billion_last_x)

# var_analyse = stack_symbol_all['var_analyse']
# all_top_15_var = var_analyse['all_top_15']
# gt_10_billion_top_15_var = var_analyse['gt_10_billion_top_15']
# le_10_billion_top_15_var = var_analyse['le_10_billion_top_15']
# print(f"\n\n过去{x_day}个工作日股价最不稳定的15只个股：")
# print_data(all_top_15_var)
# print(f"\n\n过去{x_day}个工作日股价最不稳定的15只个百亿股：")
# print_data(gt_10_billion_top_15_var)
# print(f"\n\n过去{x_day}个工作日股价最不稳定的15只个微盘股：")
# print_data(le_10_billion_top_15_var)
#
# all_last_15_var = var_analyse['all_last_15']
# gt_10_billion_last_15_var = var_analyse['gt_10_billion_last_15']
# le_10_billion_last_15_var = var_analyse['le_10_billion_last_15']
# print(f"\n\n过去{x_day}个工作日股价最稳定的15只个股：")
# print_data(all_last_15_var)
# print(f"\n\n过去{x_day}个工作日股价最稳定的15只个百亿股：")
# print_data(gt_10_billion_last_15_var)
# print(f"\n\n过去{x_day}个工作日股价最稳定的15只个微盘股：")
# print_data(le_10_billion_last_15_var)
