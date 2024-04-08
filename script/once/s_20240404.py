import sys

import numpy as np
from scipy import stats
import matplotlib.pyplot as plt

from sql_helper.bao_stock_helper import BaoStockHelper
from index.macd import Macd

code = "sh.603993"
bsh = BaoStockHelper().conn()
stock_data_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2022-10-01", day="2024-04-03")
stock_macd = Macd(k_data=stock_data_df)
dif_s_l = np.array([])
try:
    dif_s_l = stock_macd.get_dif_s_l()
except Exception as ex:
    print(f"\033[91m捕获到异常:{str(ex)} \033[0m")
    sys.exit()

dif_s_l_24 = dif_s_l.iloc[0:24].dropna().iloc[::-1].apply(lambda x: float(x))
dea_count = len(dif_s_l_24)
seq = np.array(range(dea_count))
slope, intercept, r_value, p_value, std_err = stats.linregress(seq, np.array(dif_s_l_24))
isUp = "上升趋势" if slope > 0 else "下降趋势"
print(f'过去24个交易日，斜率为: {slope} 截距:{intercept}，趋势:{isUp}')
# 根据计算得到的斜率和截距，创建拟合的y值
y_fit_24 = slope * seq + intercept

dif_s_l_7 = dif_s_l.iloc[0:7].dropna().iloc[::-1].apply(lambda x: float(x))
dea_count_7 = len(dif_s_l_7)
seq_7 = np.array(range(dea_count - dea_count_7, dea_count))
slope_7, intercept_7, r_value_7, p_value_7, std_err_7 = stats.linregress(seq_7, np.array(dif_s_l_7))
isUp_7 = "上升趋势" if slope_7 > 0 else "下降趋势"
print(f'过去7个交易日，斜率为: {slope_7} 截距:{intercept_7}，趋势:{isUp_7}')
y_fit_7 = slope_7 * seq_7 + intercept_7

# 绘制原始数据点
plt.scatter(seq, dif_s_l_24, label='Original data')

# 绘制拟合的线性曲线
plt.plot(seq, y_fit_24, color='red', label='Fitted line 24 day')
plt.plot(seq_7, y_fit_7, color='orange', label='Fitted line 7 day')


# 添加图例和标签
plt.legend()
plt.xlabel('x')
plt.ylabel('y')
plt.title('Linear Regression')

# 显示图形
plt.show()
