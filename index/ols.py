from typing import Callable
from index.index_common import IndexCommon

import pandas as pd
import numpy as np
from scipy import stats
import matplotlib.pyplot as plt


# 做最小二乘法回归，需要有 x轴 和 y轴的数据
# 提供需要拟合的数据在数据集中的key
# 需要这些数据 时间倒序且无缺失值，这符合k线值的特征
# x轴：对指定的数据列生成倒序的步长为1的range，如数据存在于 2024-03-15 -> 2024-03-11，生成的 index 为 [4,3,2,1,0]
# y轴：需要进行拟合的数据
#
# 截取所需要的 (x,y) 数据进行拟合，得到
# slope 线性回归线的斜率。它表示当 x 增加一个单位时，y 的平均预期变化量。
# intercept 线性回归线在 y 轴上的截距。它表示当 x 为 0 时，y 的预期值
# r_value x 和 y 之间的皮尔逊相关系数。它的值介于 -1 和 1 之间。当值为 1 时，表示完全的正相关；当值为 -1 时，表示完全的负相关；当值为 0 时，表示没有线性相关性
# p_value 检验 x 和 y 之间是否存在线性关系的双尾检验的 p 值。通常，如果 p 值小于某个显著性水平（如 0.05），我们会拒绝原假设（即 x 和 y 之间没有线性关系），并认为存在线性关系。
# std_err 斜率的估计标准误差。它提供了关于斜率估计值不确定性的度量。标准误差越小，对斜率的估计就越有信心。
#  todo : 对  r_value, p_value, std_err  做校验，确定回归结果是否有效
class Ols(IndexCommon):
    data: pd.DataFrame
    get_x_func: Callable[[pd.DataFrame, str], pd.Series]
    get_y_func: Callable[[pd.DataFrame, str], pd.Series]
    ols_data_key: str
    gen_record_cache: list[dict]
    max_end: int = 0

    def __init__(self, data: pd.DataFrame, ols_data_key='close',
                 get_x_func=lambda df, key: pd.Series(reversed(range(len(df[key].dropna())))),
                 get_y_func=lambda df, key: df[key].apply(lambda x: float(x))):
        super().__init__()
        self.data = data
        self.get_x_func = get_x_func
        self.get_y_func = get_y_func
        self.ols_data_key = ols_data_key
        self.gen_record_cache = []

    def x_index_key(self):
        return f"x_index_{self.ols_data_key}"

    def y_index_key(self):
        return f"y_index_{self.ols_data_key}"

    def get_x_index(self):
        key = self.x_index_key()
        x_index = self.data.get(key)
        if x_index is None:
            x_index = self.get_x_func(self.data, self.ols_data_key)
            self.data[key] = x_index
        return x_index

    def get_y_index(self):
        return self.get_y_func(self.data, self.ols_data_key)

    def _init_range(self, start: int, interval: int):
        start = start
        end = start + interval
        x_index = self.get_x_index()
        x_index_len = len(x_index.dropna())
        if end > x_index_len:
            raise ValueError(f"可获取的 x 轴坐标最大数量 {x_index_len} ,需要的最大数量 {end},超出允许的范围")
        return start, end

    def _cut_data(self, start: int, end: int):
        x_index_cut_array = np.array(self.get_x_index().iloc[start:end].iloc[::-1])
        y_index_cut_array = np.array(self.get_y_index().iloc[start:end].iloc[::-1])
        return x_index_cut_array, y_index_cut_array

    def _ols_calc(self, start, end):
        x, y = self._cut_data(start, end)
        return x, y, *stats.linregress(x=x, y=y)

    def ols(self, start=0, interval=7):
        start, end = self._init_range(interval=interval, start=start)
        x, y, slope, intercept, r_value, p_value, std_err = self._ols_calc(start, end)
        self.set_ols_to_data(slope=slope, intercept=intercept, start=start, end=end, interval=interval,
                             fit_data=np.flip((slope * x + intercept)))
        return self

    # 相同长度的每一个不同区间，其皆为相交或不交的关系，不会有相等的情况，
    # 即相同长度的不同区间的斜率等数据可以存储于同一列，其下标极为区间的第一个index
    def set_ols_to_data(self, slope, intercept, start, end, interval, fit_data):
        slope_key = f'slope_{interval}'
        intercept_key = f' intercept_{interval}'
        self.__cache_generate_record(start, end, interval)
        self.pd_df_set_to_column_batch(df=self.data, key=self.ols_result_key(start, end), index_start=start,
                                       index_end=end, value=fit_data)
        self.pd_df_set_to_column(self.data, slope_key, start, slope)
        self.pd_df_set_to_column(self.data, intercept_key, start, intercept)

    def ols_result_key(self, start, end):
        return f'ols_{self.ols_data_key}_{start}_{end}'

    def __cache_generate_record(self, start, end, interval):
        gen_record = {'start': start, 'end': end, 'interval': interval}
        self.max_end = end if end > self.max_end else self.max_end
        self.gen_record_cache.append(gen_record)

    ## 绘制生成结果
    def plot(self):
        if len(self.gen_record_cache) == 0:
            return
        plt.figure()
        x = self.get_x_index()
        plt.scatter(x.iloc[0:self.max_end], self.get_y_index().iloc[0:self.max_end])
        for gen_record in self.gen_record_cache:
            start = gen_record['start']
            end = gen_record['end']
            interval = gen_record['interval']
            ols_result_key = self.ols_result_key(start, end)
            y = self.data.get(ols_result_key).iloc[start:end]
            plt.plot(x.iloc[start:end], y, label=f'{start}_{interval}')
        # 添加图例和标签
        plt.legend()
        plt.xlabel('x')
        plt.ylabel('y')
        plt.title('Linear Regression')
        # 显示图形
        plt.show()

    def upward_fitting(self, start_step=7, start=0, jump_step=3, break_slope=0.005):
        slope_cache = []
        step = start_step
        start, end = self._init_range(start, step)
        x, y, slope, intercept, r_value, p_value, std_err = self._ols_calc(start, end)
        slope_cache.append({f'slope': slope, 'start': start, 'step': step, 'end': end})
        if slope < break_slope:
            print("个股近期处于或即将进入下降趋势：")
            print(slope_cache[-1])
            return slope_cache[-1]

        while slope > break_slope:
            step += 1
            try:
                start, end = self._init_range(start, step)
            except Exception as ex:
                print(str(ex))
                print("拟合结束，可供拟合数据均处于上升趋势")
                break
            x, y, slope, intercept, r_value, p_value, std_err = self._ols_calc(start, end)
            slope_cache.append({f'slope': slope, 'start': start, 'step': step, 'end': end})
        last = slope_cache[-1]
        print(last)
        return last
