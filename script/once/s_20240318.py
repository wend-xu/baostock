from sql_helper.xq_stock_helper import XqStockHelper
import matplotlib.pyplot as plt

xqStockHelper = XqStockHelper()
xqStockHelper.conn()
sz000651_10day = xqStockHelper.get_stock_last_x_day("SZ000651", 20)
sz000651_10day_avg = {}
sz000651_10day_diff = {}
sz000651_10day_current = {}

sz000651_10day_date = []
sz000651_10day_avg_list = []
sz000651_10day_current_list = []
sz000651_10day_bias_list = []

for i in reversed(range(10)):
    stock_data_i = sz000651_10day[i]
    last_10_still_i = sz000651_10day[i:i + 10]
    avg = sum(stock_data.current for stock_data in last_10_still_i) / 10
    sz000651_10day_avg[stock_data_i.date] = avg
    sz000651_10day_diff[stock_data_i.date] = stock_data_i.current - avg
    sz000651_10day_current[stock_data_i.date] = stock_data_i.current
    sz000651_10day_date.append(stock_data_i.date)
    sz000651_10day_avg_list.append(avg)
    sz000651_10day_current_list.append(stock_data_i.current)
    sz000651_10day_bias_list.append((stock_data_i.current - avg) / avg * 100)

for key in sz000651_10day_avg:
    print("{date} \t {avg} \t {current}".format(date=key, avg=sz000651_10day_avg[key],
                                                current=sz000651_10day_current[key]))

plt.figure()
# plt.plot(sz000651_10day_date, sz000651_10day_avg_list, label="十日均线")
# plt.plot(sz000651_10day_date, sz000651_10day_current_list, label="当日收盘价")
# plt.yticks(range(5, 18, 1))

plt.plot(sz000651_10day_date, sz000651_10day_bias_list, label="bias")
plt.yticks(range(0, 180, 1))
plt.title("avg-current")
plt.xlabel("date")
plt.ylabel("price")
plt.show()
