from index.macd import Macd
from index.ols import Ols
from sql_helper.bao_stock_helper import BaoStockHelper

# code = "sz.300037"
code = "sz.002444"
bsh = BaoStockHelper().conn()
stock_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2020-01-01", day="2024-04-03")
macd = Macd(k_data=stock_df)
dif_s_l = macd.get_dif_s_l()
ols = Ols(data=stock_df, ols_data_key="dif_12_26")
start, interval, slope, less_fit = 0, 7, 0, 0
while slope >= 0 or less_fit > 0:
    less_fit -= 1
    try:
        ols.ols(start=start, interval=interval)
        slope = stock_df[f'slope_{interval}'].dropna().iloc[-1]
        start += 1
    except Exception as ex:
        print(str(ex))
        break
ols.ols(start=0, interval=start + interval)
long_interval = (start + interval) * 2
ols.ols(start=0, interval=long_interval)
slope_long = stock_df[f'slope_{long_interval}'].dropna().iloc[-1]
trend_long = "上升" if slope_long > 0 else "下降"
date_cute = stock_df['date'].iloc[0:(start + interval)]
startDate = date_cute.iloc[-1]
endDate = date_cute.iloc[0]
print(
    f"从 {startDate} - {endDate} 期间[{code}]呈短周期({interval})上升趋势，持续时间为{endDate - startDate} ,在上升周期的两倍长周期({long_interval})处于:【{trend_long}:{slope_long}】" if start > 1 else f"在开始时间的过去{interval}处于下跌趋势")
ols.plot()
#
# fitting_result = ols.upward_fitting(start_step=7)
# upper_cut = stock_df.iloc[fitting_result['start']:fitting_result['end']]
# upper_cut['date'].iloc[-1]
print()
