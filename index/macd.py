from decimal import Decimal
from typing import Callable

import pandas as pd


class Macd:
    k_data: pd.DataFrame
    get_close_func: Callable[[pd.DataFrame, str], pd.Series]

    def __init__(self, k_data, get_close=lambda df, key='close': df[key]):
        super().__init__()
        self.k_data = k_data
        self.get_close_func = get_close

    def get_close(self) -> pd.Series:
        return self.get_close_func(self.k_data)

    # 计算过去 X 日均线
    def ma_x(self, x: int):
        rows, cols = self.k_data.shape
        series_ma_x = pd.Series()
        if rows < x:
            raise ValueError(f"计算过去 {x} 天EA失败，数据仅包含 {rows} 行，不足以计算EA")
        # 假设 k 线数据只有15天，计算MA_10 ,那只能准确计算过去 6 天的 MA 值
        k_data_close = self.get_close()
        can_calc_count = rows - x + 1
        # range 函数不包含stop，可以理解为从0开始循环can_calc_count次
        for i in reversed(range(can_calc_count)):
            k_data_close_i_range = k_data_close.iloc[i:i + x]
            i_ma_x = Decimal(k_data_close_i_range.mean())
            series_ma_x[k_data_close.index[i]] = i_ma_x
        self.k_data[f'ma{x}'] = series_ma_x
        return self

    def get_ma_x(self, x: int) -> pd.Series:
        ma_x = self.k_data.get(f'ma{x}')
        if ma_x is None:
            self.ma_x(x)
            ma_x = self.k_data[f'ma{x}']
        return ma_x

    # 计算过去 x 日均线与股价的乖离值
    def bias_x(self, x: int):
        ma_x = self.get_ma_x(x)
        close = self.get_close()
        self.k_data[f'bias{x}'] = (close - ma_x) / ma_x
        return self

    # 计算 指数移动平均线 ema为加权均线，ma为均线，ema近期股价影响更大
    def ema_x(self, x: int):
        rows, cols = self.k_data.shape
        if rows < x:
            raise ValueError(f"计算过去 {x} 天EMA失败，数据仅包含 {rows} 行，不足以计算EMA")

        series_ema_x = pd.Series()
        k_data_close = self.get_close()
        can_calc_count = rows - x
        # 使用ma作为ema的初始值
        ema_prev = Macd(k_data=self.k_data[can_calc_count:can_calc_count + x].reset_index(drop=True),
                        get_close=self.get_close_func).get_ma_x(x)[0]
        for i in reversed(range(can_calc_count)):
            # k_data_close_i_range = k_data_close.iloc[i:i + x]
            close = k_data_close.iloc[i]
            ema_i = (2 * close + (x - 1) * ema_prev) / (x + 1)
            ema_prev = ema_i
            series_ema_x[k_data_close.index[i]] = ema_i
        self.k_data[f'ema{x}'] = series_ema_x
        return self

    def get_ema_x(self, x: int) -> pd.Series:
        ema_x = self.k_data.get(f'ema{x}')
        if ema_x is None:
            self.ema_x(x)
        return self.k_data.get(f'ema{x}')

    def dif_s_l(self, sort_term: int = 12, long_term: int = 26):
        ema_sort_term = self.get_ema_x(sort_term)
        ema_long_term = self.get_ema_x(long_term)
        self.k_data[self.get_dif_s_l_key(sort_term=sort_term, long_term=long_term)] = (ema_sort_term - ema_long_term)
        return self

    def get_dif_s_l_key(self, sort_term: int = 12, long_term: int = 26):
        return f'dif_{sort_term}_{long_term}'

        # 计算长周期指数平均线和短周期指数平均的差，即趋势

    def get_dif_s_l(self, sort_term: int = 12, long_term: int = 26):
        key = self.get_dif_s_l_key(sort_term=sort_term, long_term=long_term)
        dif_s_l = self.k_data.get(key)
        if dif_s_l is None:
            self.dif_s_l(sort_term=sort_term, long_term=long_term)
        return self.k_data.get(key)

    # 计算 diff 的过去 x 日加权均线
    # 计算 dea 依赖于 计算 dif ， dea_x 本质为 dif 过去 x 日的均线
    def dea_s_l_x(self, x: int = 9, sort_term: int = 12, long_term: int = 26):

        # 截取到非 nan 的 dif 即通过k线数据可计算出得 dif 长度，可以计算出的 dea_x 为 dif 的 长度 - 9
        dif_s_l = self.get_dif_s_l(sort_term=sort_term, long_term=long_term)
        dif_s_l = self.sub_series_when_first_nan(dif_s_l)
        if len(dif_s_l) < x:
            raise ValueError(f"计算过去 {x} 天 DEA 失败，数据仅包含 {len(dif_s_l)} 行，不足以计算 DEA")

        series_dea_s_l_x = pd.Series()

        # 假设计算 dea_12_26_9 ,dif的长度为 50， 可以计算出 50 - 9 = 41 个diff
        # 使用 dif_s_l.iloc[41,50] 的数据均值 即最后9个数字作为初始值
        # dif_s_l.iloc[0,10] 到 dif_s_l.iloc[40,49] 的数据计算出 41个 diff
        can_calc_count = len(dif_s_l) - x
        # 第一个 dea 值直接使用 dif_s_l 初始日期（含）的过去 x 个 diff_s_l 值的平均值
        dea_prev = Decimal(dif_s_l.iloc[can_calc_count:can_calc_count + x].mean())
        for i in reversed(range(can_calc_count)):
            dif_i = dif_s_l.iloc[i]
            dea_i = (dif_i * 2 + dea_prev * (x - 1)) / (x + 1)
            series_dea_s_l_x[dif_s_l.index[i]] = dea_i
            dea_prev = dea_i

        self.k_data[f'dea_{sort_term}_{long_term}_{x}'] = series_dea_s_l_x
        return self

    def get_dea_s_l_x(self, x: int = 9, sort_term: int = 12, long_term: int = 26) -> pd.Series:
        key = f'dea_{sort_term}_{long_term}_{x}'
        dea_s_l_x = self.k_data.get(key)
        if dea_s_l_x is None:
            self.dea_s_l_x(x=x, long_term=long_term, sort_term=sort_term)
        return self.k_data.get(key)

    @staticmethod
    def sub_series_when_first_nan(s: pd.Series):
        first_nan_idx = (s.isnull()).idxmax()
        if first_nan_idx is not None:  # 确保存在 NaN
            return s.loc[:first_nan_idx]
        else:
            return s  # 如果没有 NaN，则返回整个 Series
