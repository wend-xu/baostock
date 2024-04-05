from decimal import Decimal

from matplotlib.ticker import MaxNLocator

from index.macd import Macd
from sql_helper.bao_stock_helper import BaoStockHelper
from sql_helper.xq_stock_helper import XqStockHelper
import pandas as pd
import matplotlib.pyplot as plt
import baostock as bs

x = 20
baostockHelper = BaoStockHelper()
baostockHelper.conn()
sz000651_last_x_day = baostockHelper.get_stock_last_x_day_k(code="sz.002085", last_x_day=120)
sz000651_last_x_day_df = pd.DataFrame(sz000651_last_x_day)
baostockHelper.dis_conn()

macd = Macd(k_data=sz000651_last_x_day_df,
            get_close=lambda df: df['close'])

macd.dif_s_l(long_term=26, sort_term=12)

y_index = sz000651_last_x_day_df['date'].iloc[0:90].sort_index(ascending=False)
dif_12_26 = macd.get_dif_s_l(sort_term=12, long_term=26).iloc[0:90].sort_index(ascending=False)
ema_12 = macd.get_ema_x(12).iloc[0:90].sort_index(ascending=False)
ema_26 = macd.get_ema_x(26).iloc[0:90].sort_index(ascending=False)

ma_12 = macd.get_ma_x(12).iloc[0:90].sort_index(ascending=False)
ma_26 = macd.get_ma_x(26).iloc[0:90].sort_index(ascending=False)
dif_ma_12_26 = ma_12 - ma_26

dea_12_26_9 = macd.get_dea_s_l_x(sort_term=12, long_term=26, x=9).iloc[0:90].sort_index(ascending=False)

plt.figure()
plt.plot(y_index, dif_12_26, label="dif_12_26")
plt.plot(y_index, dea_12_26_9, label="dea_12_26_9")
# plt.plot(y_index, ema_12, label="ema12")
# plt.plot(y_index, ema_26, label="ema26")
# plt.plot(y_index, diff_12_26, label="diff_12_26")
# plt.plot(y_index, ma_12, label="ma_12")
# plt.plot(y_index, ma_26, label="ma_26")

plt.legend()
plt.yticks(range(0, 2, 1))
plt.title("macd-date")
plt.xlabel("date")
plt.ylabel("macd")
plt.gca().xaxis.set_major_locator(MaxNLocator(8))
plt.show()
