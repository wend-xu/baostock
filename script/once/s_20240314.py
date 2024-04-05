from sql_helper.xq_stock_helper import XqStockHelper


def print_data(data_list):
    for last_x_day in data_list:
        print(
            "{symbol}:{name} 共得到 {total} 条记录，涨幅是 {last_x_day_percent} ,上涨天数： {last_x_day_upper} ,涨幅方差：{last_x_day_var} ，最新市值：{float_market_capital} 亿".format(
                **last_x_day))


def print_data_inflow(data_list):
    for last_x_day in data_list:
        print(
            "{symbol}:{name} 共得到 {total} 条记录，涨幅是 {last_x_day_percent} ，北向流入 {last_x_day_north_net_inflow} 亿 ,流入天数： {last_x_day_north_net_inflow_day} ,主力流入： {last_x_day_main_net_inflow} 亿,主力流入天数：{last_x_day_north_net_inflow_day} , 总成交额：{last_x_day_amount} 亿，日均成交额： {last_x_day_amount_avg} 亿,最新市值：{float_market_capital} 亿".format(
                **last_x_day))


def print_analyse_result(result, top_x, print_fun):
    all_top_x = result[f'all_top_{top_x}']
    gt_10_billion_top_x = result[f'gt_10_billion_top_{top_x}']
    le_10_billion_top_x = result[f'le_10_billion_top_{top_x}']
    print(f"\n\n过去{x_day}最高的{top_x}只个股：")
    print_fun(all_top_x)
    print(f"\n\n过去{x_day}最高的{top_x}只个百亿股：")
    print_fun(gt_10_billion_top_x)
    print(f"\n\n过去{x_day}最高的{top_x}只个微盘股：")
    print_fun(le_10_billion_top_x)

    all_last_x = result[f'all_last_{top_x}']
    gt_10_billion_last_x = result[f'gt_10_billion_last_{top_x}']
    le_10_billion_last_x = result[f'le_10_billion_last_{top_x}']
    print(f"\n\n过去{x_day}最低的{top_x}只个股：")
    print_fun(all_last_x)
    print(f"\n\n过去{x_day}最低的{top_x}只个百亿股：")
    print_fun(gt_10_billion_last_x)
    print(f"\n\n过去{x_day}最低的{top_x}只个微盘股：")
    print_fun(le_10_billion_last_x)


xqStockHelper = XqStockHelper()
xqStockHelper.conn()
x_day = 3
top_x_20 = 20
stack_symbol_all = xqStockHelper.analyse_all_last_x_day(last_x_day=x_day, top_x=top_x_20, end_date='2024-03-19')
percent_analyse = stack_symbol_all['percent_analyse']
print("\n\n涨跌幅分析\n：")
print_analyse_result(percent_analyse, top_x=top_x_20, print_fun=print_data)

north_net_inflow_analyse = stack_symbol_all['north_net_inflow_analyse']
print("\n\n北向分析：\n")
print_analyse_result(north_net_inflow_analyse, top_x=top_x_20, print_fun=print_data_inflow)

main_net_inflow_analyse = stack_symbol_all['main_net_inflow_analyse']
print("\n\n主力分析：\n")
print_analyse_result(main_net_inflow_analyse, top_x=top_x_20, print_fun=print_data_inflow)

amount_analyse = stack_symbol_all['amount_analyse']
print("\n\n成交分析：\n")
print_analyse_result(amount_analyse, top_x=top_x_20, print_fun=print_data_inflow)

