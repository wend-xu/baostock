from datetime import datetime
from decimal import Decimal
from typing import Union, List, Dict

import mysql
import pandas as pd
from mysql.connector.abstracts import MySQLCursorAbstract, MySQLConnectionAbstract
from mysql.connector.pooling import PooledMySQLConnection

import numpy as np

from entity.xq_stock_data_day import XqStockDataDay
from sql_helper.common_helper import CommonHelper


class XqStockHelper(CommonHelper):
    connection: Union[PooledMySQLConnection, MySQLConnectionAbstract]
    cursor: MySQLCursorAbstract

    def __init__(self):
        super().__init__()

    def conn(self, host="127.0.0.1", user="root", password="qqaazz321", database="stock"):
        self.connection = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        self.cursor = self.connection.cursor(dictionary=True)
        return self

    def dis_conn(self):
        self.cursor.close()
        self.connection.close()
        return self

    # 获取过去X天的股票数据
    def get_stock_last_x_day(self, symbol: str, last_x_day=7) -> List[XqStockDataDay]:
        today_yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")
        get_last_x_day_sql = """
                   select * from xq_stock_data_day 
                    where symbol= '{symbol}' and date <= '{last_date}' 
                    order by date desc limit {x_day}
               """.format(symbol=symbol, last_date=today_yyyy_mm_dd, x_day=last_x_day)
        self.cursor.execute(get_last_x_day_sql)
        last_x_day_data = self.cursor.fetchall()
        return [XqStockDataDay.to_obj(one) for one in last_x_day_data]

    def get_stock_latest_info(self, symbol: str) -> XqStockDataDay:
        get_latest_sql = """
                          select * from xq_stock_data_day 
                           where symbol= '{symbol}' 
                           order by date desc limit 1
                      """.format(symbol=symbol, )

        self.cursor.execute(get_latest_sql)
        one = self.cursor.fetchall()
        return XqStockDataDay.to_obj(one[0])

        # 获取过去x天的股票统计指标

    def last_x_day_index(self, symbol: str, last_x_day=1) -> Dict[str, object]:
        last_x_day_list = self.get_stock_last_x_day(symbol, last_x_day)
        return self.analyse_stock_data(last_x_day_list)

    # 获取当前交易日的股票列表
    def get_all_stock_symbol_for_date(self, date=datetime.now().strftime("%Y-%m-%d")) -> list[str]:
        sql = f"""
            select symbol from xq_stock_data_day
                    where date = '{date}'
        """
        self.cursor.execute(sql)
        symbol_all = self.cursor.fetchall()
        return [one['symbol'] for one in symbol_all]

    # 生成所有股票的日分析报告
    def analyse_all_last_x_day(self, last_x_day: int, top_x=15, end_date=datetime.now().strftime("%Y-%m-%d")) \
            -> Dict[str, Dict[str, Dict]]:
        result = {}
        all_symbol = self.get_all_stock_symbol_for_date(end_date)
        ### 所有数据分析
        all_analyse_data = {}
        for symbol in all_symbol:
            stock_last_x_day = self.get_stock_last_x_day(symbol, last_x_day=last_x_day)
            stock_last_x_day_analyse = self.analyse_stock_data(stock_last_x_day)
            all_analyse_data[symbol] = stock_last_x_day_analyse
        result['all'] = all_analyse_data
        result['percent_analyse'] = self.analyse_stock_percent(data=all_analyse_data, x=top_x)
        result['var_analyse'] = self.analyse_stock_var(data=all_analyse_data, x=top_x)
        result['north_net_inflow_analyse'] = self.analyse_stock_north_net_inflow(data=all_analyse_data, x=top_x)
        result['main_net_inflow_analyse'] = self.analyse_stock_main_net_inflow(data=all_analyse_data, x=top_x)
        result['amount_analyse'] = self.analyse_stock_amount(data=all_analyse_data, x=top_x)
        return result

    ### 过去x天的所有个股分析
    def analyse_stock_data(self, data: List[XqStockDataDay]) -> Dict[str, Dict]:
        if data is None or len(data) == 0:
            return {}
        last_x_day_percent = sum(one.percent for one in data)
        last_x_day_north_net_inflow = sum(one.north_net_inflow for one in data)
        last_x_day_amount = sum(one.amount for one in data)
        last_x_day_amount_avg = sum(one.amount for one in data) / len(data)
        last_x_day_main_net_inflow = sum(one.main_net_inflows for one in data)

        last_x_day_upper = sum(1 if one.percent > 0 else 0 for one in data)
        last_x_day_north_net_inflow_day = sum(1 if one.north_net_inflow > 0 else 0 for one in data)
        last_x_day_north_main_net_inflow_day = sum(1 if one.main_net_inflows > 0 else 0 for one in data)

        last_x_day_var = np.array([one.percent for one in data]).var()

        return {'last_x_day_percent': last_x_day_percent,
                'last_x_day_north_net_inflow': last_x_day_north_net_inflow / Decimal('100000000'),
                'last_x_day_amount': last_x_day_amount / Decimal('100000000'),
                'last_x_day_amount_avg': Decimal(last_x_day_amount_avg) / Decimal('100000000'),
                'last_x_day_main_net_inflow': last_x_day_main_net_inflow / Decimal('100000000'),

                'last_x_day_upper': last_x_day_upper,
                'last_x_day_north_net_inflow_day': last_x_day_north_net_inflow_day,
                'last_x_day_north_main_net_inflow_day': last_x_day_north_main_net_inflow_day,

                'last_x_day_var': last_x_day_var,
                'total': len(data), 'float_market_capital': data[0].float_market_capital / Decimal('100000000'),
                'name': data[0].name, 'symbol': data[0].symbol}

    ### 过去x天的所有个股分析根据涨跌幅过滤
    def analyse_stock_percent(self, data: Dict[str, Dict], x=15) -> Dict[str, List[Dict[str, Dict]]]:
        return self.analyse_top_and_last_x(x=x, data=data,
                                           sort_func=lambda day_one: 0 if day_one[1]['last_x_day_percent'] is None else
                                           day_one[1]['last_x_day_percent'])

    ### 过去x天的所有个股分析根据方差过滤
    def analyse_stock_var(self, data: Dict[str, Dict], x=15) -> Dict[str, List[Dict[str, Dict]]]:
        return self.analyse_top_and_last_x(x=x, data=data,
                                           sort_func=lambda day_one: 0 if day_one[1]['last_x_day_var'] is None else
                                           day_one[1]['last_x_day_var'])

    def analyse_stock_north_net_inflow(self, data: Dict[str, Dict], x=15) -> Dict[str, List[Dict[str, Dict]]]:
        return self.analyse_top_and_last_x(x=x, data=data,
                                           sort_func=lambda day_one: 0 if day_one[1][
                                                                              'last_x_day_north_net_inflow'] is None else
                                           day_one[1]['last_x_day_north_net_inflow'])

    def analyse_stock_main_net_inflow(self, data: Dict[str, Dict], x=15) -> Dict[str, List[Dict[str, Dict]]]:
        return self.analyse_top_and_last_x(x=x, data=data,
                                           sort_func=lambda day_one: 0 if day_one[1][
                                                                              'last_x_day_main_net_inflow'] is None else
                                           day_one[1]['last_x_day_main_net_inflow'])

    def analyse_stock_amount(self, data: Dict[str, Dict], x=15) -> Dict[str, List[Dict[str, Dict]]]:
        return self.analyse_top_and_last_x(x=x, data=data,
                                           sort_func=lambda day_one: 0 if day_one[1]['last_x_day_amount'] is None else
                                           day_one[1]['last_x_day_amount'])

    def analyse_top_and_last_x(self, x: int, sort_func, data: Dict[str, Dict]) -> Dict[str, List[Dict[str, Dict]]]:
        data_sort = list(data.items())
        data_sort.sort(
            key=sort_func,
            reverse=True)
        all_top_x = []
        gt_10_billion_top_x = []
        le_10_billion_top_x = []
        for data_one in data_sort:
            if len(all_top_x) <= x:
                all_top_x.append(data[data_one[1]['symbol']])
            if len(gt_10_billion_top_x) <= x and data_one[1]['float_market_capital'] > 100:
                gt_10_billion_top_x.append(data[data_one[1]['symbol']])
            if len(le_10_billion_top_x) <= x and data_one[1]['float_market_capital'] <= 100:
                le_10_billion_top_x.append(data[data_one[1]['symbol']])
            if (x * 3) == (len(all_top_x) + len(gt_10_billion_top_x) + len(le_10_billion_top_x)):
                break

        all_last_x = []
        gt_10_billion_last_x = []
        le_10_billion_last_x = []
        for data_one in reversed(data_sort):
            if len(all_last_x) <= x:
                all_last_x.append(data[data_one[1]['symbol']])
            if len(gt_10_billion_last_x) <= x and data_one[1]['float_market_capital'] > 100:
                gt_10_billion_last_x.append(data[data_one[1]['symbol']])
            if len(le_10_billion_last_x) <= x and data_one[1]['float_market_capital'] <= 100:
                le_10_billion_last_x.append(data[data_one[1]['symbol']])
            if (x * 3) == (len(all_last_x) + len(gt_10_billion_last_x) + len(le_10_billion_last_x)):
                break

        return {
            f"all_top_{x}": all_top_x,
            f"gt_10_billion_top_{x}": gt_10_billion_top_x,
            f"le_10_billion_top_{x}": le_10_billion_top_x,
            f"all_last_{x}": all_last_x,
            f"gt_10_billion_last_{x}": gt_10_billion_last_x,
            f"le_10_billion_last_{x}": le_10_billion_last_x,
        }

    def analyse_stock_in_interval(self, symbol: str, start_date: str = None, interval: int = 7):
        if start_date is None:
            start_date = datetime.now().strftime("%Y-%m-%d")
        sql = f"""
            select * from xq_stock_data_day where symbol = "{symbol}" and   `date` <= "{start_date}" order by `date` desc limit {interval}
        """
        self.cursor.execute(sql)
        select_result = self.cursor.fetchall()
        if len(select_result) == 0:
            return {}
        data_df = pd.DataFrame(select_result)
        north_net_inflow_3_days = self.sum_series(data=data_df['north_net_inflow'], length=3)
        north_net_inflow_7_days = self.sum_series(data=data_df['north_net_inflow'], length=7)
        north_net_inflow_interval = self.sum_series(data=data_df['north_net_inflow'], length=interval)
        main_net_inflow_3_days = self.sum_series(data=data_df['main_net_inflows'], length=3)
        main_net_inflow_7_days = self.sum_series(data=data_df['main_net_inflows'], length=7)
        main_net_inflow_interval = self.sum_series(data=data_df['main_net_inflows'], length=interval)
        amount_3_days = self.sum_series(data=data_df["amount"], length=3)
        amount_7_days = self.sum_series(data=data_df["amount"], length=7)
        amount_interval = self.sum_series(data=data_df["amount"], length=interval)
        latest = select_result[0]
        return {
            'north_net_inflow': latest.get('north_net_inflow'),
            'north_net_inflow_3_days': north_net_inflow_3_days,
            'north_net_inflow_7_days': north_net_inflow_7_days,
            'north_net_inflow_interval': north_net_inflow_interval,
            'main_net_inflow': latest.get('main_net_inflow'),
            'main_net_inflow_3_days': main_net_inflow_3_days,
            'main_net_inflow_7_days': main_net_inflow_7_days,
            'main_net_inflow_interval': main_net_inflow_interval,
            'amount': latest.get('amount'),
            'amount_3_days': amount_3_days,
            'amount_7_days': amount_7_days,
            'amount_interval': amount_interval,
            'pb_ttm': latest.get('pb_ttm'),
            'current_year_percent': latest.get('current_year_percent'),
            'float_market_capital': latest.get('float_market_capital'),
            'roe_ttm': latest.get('roe_ttm'),
            'dividend_yield': latest.get('dividend_yield'),
            'income_cagr': latest.get('income_cagr'),
            'eps': latest.get("eps"),
            'turnover_rate': latest.get('turnover_rate'),
            'limitup_days': latest.get('limitup_days'),
            'xq_followers': latest.get('followers')
        }
