from datetime import date, datetime

import pandas as pd
from scipy.stats import pearsonr, spearmanr
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt

from sql_helper.bao_stock_helper import BaoStockHelper
from sql_helper.bao_stock_index_helper import BaoStockIndexHelper


class Related:
    bs: BaoStockHelper
    bi: BaoStockIndexHelper

    def __init__(self):
        super().__init__()
        self.bs = BaoStockHelper()
        self.bi = BaoStockIndexHelper()

    def conn(self, host="127.0.0.1", user="root", password="qqaazz321", database="stock"):
        self.bs.conn(host=host, user=user, password=password, database=database)
        self.bi.conn(host=host, user=user, password=password, database=database)
        return self

    def dis_conn(self):
        self.bi.dis_conn()
        self.bs.dis_conn()
        return self

    def pre_handle(self, data_df: pd.DataFrame) -> pd.DataFrame:
        data_df['date'] = pd.to_datetime(data_df['date'])
        data_df['close'] = data_df['close'].apply(float)
        data_df = data_df.set_index('date')
        data_df.dropna()
        return data_df

    def align(self, data_df_1: pd.DataFrame, data_df_2: pd.DataFrame):
        data_df_1 = data_df_1.reindex(sorted(data_df_1.index.union(data_df_2.index)), fill_value=None)
        data_df_2 = data_df_2.reindex(sorted(data_df_2.index.union(data_df_1.index)), fill_value=None)
        return data_df_1, data_df_2

    def pearsonr(self, data_df_1: pd.DataFrame, data_df_2: pd.DataFrame):
        data_df_1 = self.pre_handle(data_df_1)
        data_df_2 = self.pre_handle(data_df_2)
        data_df_1, data_df_2 = self.align(data_df_1, data_df_2)
        pearson_corr, _ = pearsonr(data_df_1['close'], data_df_2['close'])
        return pearson_corr

    def spearmanr(self, data_df_1: pd.DataFrame, data_df_2: pd.DataFrame):
        data_df_1 = self.pre_handle(data_df_1)
        data_df_2 = self.pre_handle(data_df_2)
        data_df_1, data_df_2 = self.align(data_df_1, data_df_2)
        spearman_corr, _ = spearmanr(data_df_1['close'], data_df_2['close'])
        return spearman_corr

    # todo 直接在 rele_stock_and_index 返回所要的数据， relePlot 支持多指数比较 ，增加 releReportOne 计算个股和多个指数的比较 增加releReport生成关联性报告
    def rele_stock_and_index(self, stock_code: str, index_code: str, last_x_day: int = 14, latest_date: str = None):
        latest_date = datetime.now().strftime("%Y-%m-%d") if latest_date is None else latest_date
        data_df_stock = self.bs.get_stock_last_x_day_k_as_df(code=stock_code, last_x_day=last_x_day,
                                                             last_day=latest_date)
        data_df_index = self.bi.get_index_last_x_day_as_df(index_code=index_code, x=last_x_day, last_day=latest_date)
        index_date = data_df_index['date']
        start_date = index_date[0]
        end_date = index_date[len(index_date) - 1]
        return (self.pearsonr(data_df_index, data_df_stock),
                self.spearmanr(data_df_index, data_df_stock),
                data_df_stock, data_df_index, index_date, start_date, end_date)

    def rele_stock_and_multi_index(self, stock_code: str, index_code_list: list, last_x_day: int = 14,
                                   latest_date: str = None) -> list[dict]:
        latest_date = datetime.now().strftime("%Y-%m-%d") if latest_date is None else latest_date
        result_list = []
        data_df_stock = self.bs.get_stock_last_x_day_k_as_df(code=stock_code, last_x_day=last_x_day,
                                                             last_day=latest_date)
        if len(data_df_stock) < last_x_day:
            print(
                f"股票[{stock_code}]在[{latest_date}]及之前共[{len(data_df_stock)}]条记录，小于需要的[{last_x_day}],忽略")
            return []

        stock_range_date = data_df_stock['date']
        start_date, end_date = stock_range_date[0], stock_range_date[len(stock_range_date) - 1]
        for index_code in index_code_list:
            data_df_index = self.bi.get_index_last_x_day_as_df(index_code=index_code, x=last_x_day,
                                                               last_day=latest_date)
            result_list.append({'stock_code': stock_code, 'index_code': index_code,
                                'pearsonr': self.pearsonr(data_df_index, data_df_stock),
                                'spearmanr': self.spearmanr(data_df_index, data_df_stock),
                                'data_df_stock': data_df_stock, 'data_df_index': data_df_index,
                                'index_date': stock_range_date, 'start_date': start_date, 'end_date': end_date})
        return result_list

    def rele_plot_multi_index(self, stock_code: str, index_code_list: list, last_x_day: int = 14):
        rele_data_list = self.rele_stock_and_multi_index(stock_code=stock_code, index_code_list=index_code_list,
                                                         last_x_day=last_x_day)
        plt.figure()
        rele_first = rele_data_list.pop(0)
        x = rele_first['index_date']
        y_index = rele_first['data_df_index']['close']
        y_base_factor = y_index[len(y_index) - 1]
        y_stock = rele_first['data_df_stock']['close'] * self.scale_factor(y_base_factor, rele_first['data_df_stock'])
        plt.scatter(x, y_index)
        plt.plot(x, y_index, label=rele_first['index_code'])
        plt.plot(x, y_stock, label=rele_first['stock_code'])
        for rele_data in rele_data_list:
            y_index_other = rele_data['data_df_index']['close'] * self.scale_factor(y_base_factor,
                                                                                    rele_data['data_df_index'])
            plt.plot(x, y_index_other, label=rele_data['index_code'])
        plt.legend()
        plt.xlabel('date')
        plt.ylabel('point')
        plt.show()

    def scale_factor(self, y_base_factor, data_df):
        close = data_df['close']
        return y_base_factor / close[len(close) - 1]

    def rele_plot(self, stock_code: str, index_code: str, last_x_day: int = 14):
        pearsonr_corr, spearmanr_corr, data_df_stock, data_df_index, index_date, start_date, end_date = (
            self.rele_stock_and_index(stock_code, index_code, last_x_day))
        print(
            f"个股：{stock_code} 与指数： {index_code} 在{start_date}到{end_date}的{len(index_date)}条记录皮尔逊相关系数为{pearsonr_corr} , 斯皮尔曼秩相关系数为 {spearmanr_corr}")
        plt.figure()
        x = index_date
        y_index = data_df_index['close']
        y_stock = data_df_stock['close'] * (y_index[0] / data_df_stock['close'][0])

        plt.scatter(x, y_index)
        plt.plot(x, y_index, label='')
        plt.plot(x, y_stock)
        plt.legend()
        plt.xlabel('date')
        plt.ylabel('point')
        plt.show()

    def rele_stock_and_multi_index_as_report(self, stock_code: str, index_code_list: list,
                                             last_x_day: int = 14, latest_date: str = None) -> dict | None:
        latest_date = datetime.now().strftime("%Y-%m-%d") if latest_date is None else latest_date
        rele_multi: list[dict] = self.rele_stock_and_multi_index(stock_code=stock_code, index_code_list=index_code_list,
                                                                 last_x_day=last_x_day, latest_date=latest_date)
        if len(rele_multi) == 0:
            return None

        data_df_stock: pd.DataFrame = rele_multi[0]['data_df_stock']
        data_df_stock_close = data_df_stock['close']
        data_df_stock_date = data_df_stock['date']
        data_df_stock_len = len(data_df_stock_close)
        close_latest = data_df_stock_close[0]
        # 开始结束日期 股票编码 开始结束收盘价格
        report = {'stock_code': stock_code,
                  'start_date': data_df_stock_date[data_df_stock_len - 1].strftime("%Y-%m-%d"),
                  'end_date': data_df_stock_date[0].strftime("%Y-%m-%d"), 'close_latest': close_latest,
                  'close_farthest': data_df_stock_close[data_df_stock_len - 1]}
        # 3、5、7 日涨幅
        close_3_day = data_df_stock_close[2] if data_df_stock_len >= 3 else 0
        close_5_day = data_df_stock_close[4] if data_df_stock_len >= 5 else 0
        close_7_day = data_df_stock_close[6] if data_df_stock_len >= 7 else 0
        upper_percent_3_day = (close_latest - close_3_day) / close_3_day * 100 if close_3_day != 0 else 0
        upper_percent_5_day = (close_latest - close_5_day) / close_5_day * 100 if close_5_day != 0 else 0
        upper_percent_7_day = (close_latest - close_7_day) / close_7_day * 100 if close_7_day != 0 else 0
        report['close_latest'] = close_latest
        report['close_3_day'] = close_3_day
        report['close_5_day'] = close_5_day
        report['close_7_day'] = close_7_day
        report['upper_3_day'] = upper_percent_3_day
        report['upper_5_day'] = upper_percent_5_day
        report['upper_7_day'] = upper_percent_7_day

        for rele in rele_multi:
            index_code: str = rele['index_code']
            index_code_sql_key = index_code.replace(".", "").replace(":", "") + '_'
            index_data = rele['data_df_index']

            report[index_code_sql_key + 'pearsonr'] = rele['pearsonr']
            report[index_code_sql_key + 'spearmanr'] = rele['spearmanr']
            report[index_code_sql_key + 'close_farthest'] = index_data['close'][len(index_data) - 1]
            report[index_code_sql_key + 'close_latest'] = index_data['close'][0]
            # 开始结束的百分比

        return report
