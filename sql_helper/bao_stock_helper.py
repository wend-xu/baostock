import decimal
from typing import List, Union, Dict

import baostock as bs
import mysql.connector
import pandas as pd
from datetime import datetime

from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.types import RowType, RowItemType
from sqlalchemy import create_engine

from sql_helper.insert_sql_helper import dict_to_mysql_insert


class BaoStockHelper:
    connection: Union[PooledMySQLConnection, MySQLConnectionAbstract]
    cursor: MySQLCursorAbstract

    def __init__(self):
        super().__init__()

    def conn(self):
        self.connection = mysql.connector.connect(
            host="127.0.0.1", user="root", password="qqaazz321", database="stock"
        )
        self.cursor = self.connection.cursor(dictionary=True)

        return self

    def conn_bs(self):
        bs.login()
        return self

    def dis_conn(self):
        self.cursor.close()
        self.connection.close()
        return self

    def dis_conn_bs(self):
        bs.logout()
        return self

    def _execute_query_sql(self, sql):
        print(f"开始执行sql查询语句: \n {sql}")
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    def _execute_insert_sql(self, sql, values, commit=False):
        print(f"开始执行sql插入语句: \n {sql}")
        self.cursor.execute(sql, values)
        if commit:
            self.connection.commit()

    def _execute_delete_sql(self, sql, commit=False):
        print(f"即将执行sql删除语句: \n {sql}")
        self.cursor.execute(sql)
        if commit:
            self.connection.commit()

    # 获取过去 x 天的 k 线数据
    def get_stock_last_x_day_k(self, code: str, last_x_day: int) -> List[Union[RowType, Dict[str, RowItemType]]]:
        today_yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")
        get_last_x_day_sql = """
            select * from bs_stock_data_day_k where code = '{code}' and date <= '{last_date}' order by date desc limit {x_day}
        """.format(code=code, last_date=today_yyyy_mm_dd, x_day=last_x_day)
        return self._execute_query_sql(get_last_x_day_sql)

    def get_stock_last_x_day_k_as_df(self, code: str, last_x_day: int) -> pd.DataFrame:
        return pd.DataFrame(self.get_stock_last_x_day_k(code=code, last_x_day=last_x_day))

    def get_stock_last_x_day_k_temp(self, code: str, last_x_day: int) -> List[Union[RowType, Dict[str, RowItemType]]]:
        today_yyyy_mm_dd = datetime.now().strftime("%Y-%m-%d")
        get_last_x_day_sql = """ 
               select * from bs_stock_data_k_temp where code = '{code}' and date <= '{last_date}' order by date desc limit {x_day}
           """.format(code=code, last_date=today_yyyy_mm_dd, x_day=last_x_day)
        return self._execute_query_sql(get_last_x_day_sql)

    def get_stock_date_range_k_temp(self, code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        get_stock_date_range_day_k_temp_sql = f"""
            select * from bs_stock_data_k_temp where code = '{code}' and date between '{start_date}' and '{day}' order by date desc 
        """
        return self._execute_query_sql(sql=get_stock_date_range_day_k_temp_sql)

    # 获取指定时间范围的 k 线数据、
    def get_stock_date_in_date_range(self, code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        get_stock_date_range_day_k_temp_sql = f"""
            select * from bs_stock_data_day_k where code = '{code}' and date between '{start_date}' and '{day}' order by date desc 
        """
        return self._execute_query_sql(sql=get_stock_date_range_day_k_temp_sql)

    def get_stock_date_in_date_range_as_df(self, code: str, day=None, start_date=None):
        return pd.DataFrame(self.get_stock_date_in_date_range(code=code, day=day, start_date=start_date))

    # 获取指定时间范围的临时 k 线数据、
    def get_all_stock_temp_in_date_range(self, start_date, end_date) -> List[Union[RowType, Dict[str, RowItemType]]]:
        get_stock_all_in_date_range_sql = f"""
            select * from bs_stock_data_k_temp where date between '{start_date}' and '{end_date}' order by date desc   
        """
        return self._execute_query_sql(get_stock_all_in_date_range_sql)

    # 获取所有股票过去 x 天的累积涨幅 使用指数表来定位时间范围
    def get_all_stock_last_x_day_percent(self, last_x_day: int) -> List[Dict[str, RowItemType]]:
        get_index_last_x_day_sql = f"""
                    select * from bs_index_data_day_k where code = 'sh.000001'  order by date desc limit {last_x_day}; 
        """
        last_x_day_sh00001 = self._execute_query_sql(get_index_last_x_day_sql)
        last_date = last_x_day_sh00001[0]['date']
        first_date = last_x_day_sh00001[len(last_x_day_sh00001) - 1]['date']
        get_last_x_day_percent_sql = """
        select statis.*,bd.code_name, bd.ipoDate, bd.outDate, bd.`type`, bd.status, bd.industry, bd.industryClassification
            from (
                select code,sum(pctChg) as percent from bs_stock_data_day_k where date between  '{first_date}' and '{last_date}' group by code 
            ) statis
        left join bs_stock_base_data bd on statis.code =bd.code ;
        """.format(first_date=first_date, last_date=last_date)
        return self._execute_query_sql(get_last_x_day_percent_sql)

    # 获取指定日期的所有股票k线数据
    def get_all_stock_with_date(self, date=None) -> pd.DataFrame:
        date = date if date is not None else datetime.now().strftime("%Y-%m-%d")
        get_all_with_day_sql = f"""
            select * from bs_stock_data_day_k where date = '{date}';
        """
        data = self._execute_query_sql(get_all_with_day_sql)
        return pd.DataFrame(data)

    def get_base_stock_date(self, code) -> dict:
        get_base_stock_date_sql = f"""
            select * from bs_stock_base_data where code = "{code}";
        """
        data = self._execute_query_sql(get_base_stock_date_sql)
        return {} if len(data) == 0 else data[0]

    # 过去 x 天累积涨幅超过指定数值是的股票并按行业分类
    def group_by_industry_and_ge_percent(self, last_x_day: int, least_percent: decimal) \
            -> Dict[str, List[Dict[str, RowItemType]]]:
        all_stock_last_x_day_percent = self.get_all_stock_last_x_day_percent(last_x_day=last_x_day)
        all_stock_last_x_day_percent.sort(key=lambda day_one: 0 if day_one['percent'] is None else day_one['percent'],
                                          reverse=True)
        last_x_day_ge_least_percent = [one for one in all_stock_last_x_day_percent if
                                       one['percent'] is not None and one['percent'] >= least_percent]
        group_last_x_day_ge_least_percent = {}
        for item in last_x_day_ge_least_percent:
            industry_list = group_last_x_day_ge_least_percent.get(item['industry'])
            if industry_list is None:
                industry_list_init = [item]
                group_last_x_day_ge_least_percent[item['industry']] = industry_list_init
            else:
                industry_list.append(item)
        return group_last_x_day_ge_least_percent

    # 从 baostock 同步数据到本地临时表
    def sync_one_stock_k_day_to_temp(self, code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        k_data = bs.query_history_k_data_plus(code=code,
                                              fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                                              start_date=start_date, end_date=day).get_data()
        print(f"{code} 获取到时间段 {start_date} - {day} 数据共 {k_data.shape[0]} 条")
        engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
        k_data.to_sql('bs_stock_data_k_temp', engine, if_exists='append', index=False)
        return k_data

    def sync_all_stock_k_day_to_temp(self, day=None, start_date=None):
        start = datetime.now()
        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        print(f"开始时间：{start_str}")
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        # 获取指定日期的指数、股票数据
        stock_df = bs.query_all_stock(day).get_data()
        all_count = stock_df.shape[0]
        if all_count == 0:
            print("获取数据条目数为0，结束执行")
            return self
        stock_df = stock_df[
            ~(stock_df['code'].str.lower().str.startswith(("bj", "sh.000", 'sz.399')))
            & ~(stock_df['code_name'].str.lower().str.contains('st'))]
        sync_count = stock_df.shape[0]
        print(f"获得证券总数：{all_count} , 剔除北交所、ST以及指数后需要处理数目: {sync_count}")
        data_df = pd.DataFrame()

        handle_count = 0
        for code in stock_df["code"]:
            percent = handle_count / sync_count * 100
            print(
                f"开始处理第 {handle_count} 条证券代码:{code} ，进度：{percent:.2f}  %，已耗时： {datetime.now() - start} ")
            k_data = bs.query_history_k_data_plus(code=code,
                                                  fields="date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST",
                                                  start_date=start_date, end_date=day).get_data()
            data_df = pd.concat([data_df, k_data])
            handle_count += 1
        end_get_data = datetime.now()
        end_str = end_get_data.strftime("%Y-%m-%d %H:%M:%S")
        print(f"获取数据完成，结束时间：{end_str} ,耗时：{end_get_data - start} ,开始执行数据入库")
        engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
        data_df.to_sql('bs_stock_data_k_temp', engine, if_exists='append', index=False)
        end_write_db = datetime.now()
        end_write_db_str = end_write_db.strftime("%Y-%m-%d %H:%M:%S")
        print(f"入库数据完成，结束时间：{end_write_db_str} ,耗时：{end_write_db - end_get_data} ,开始执行数据入库")
        print(f"处理完成，总耗时：{end_get_data - start}")
        print(data_df)
        return self

    def _init_date_range(self, start_date=None, day=None):
        if day is None or day == "":
            day = datetime.now().strftime("%Y-%m-%d")
        if start_date is None or start_date == "":
            start_date = day
        return start_date, day

    # 从临时k线表复制数据k线表
    def cp_from_temp(self, day=None, start_date=None):
        start = datetime.now()
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        all_temp_k_info = self.get_all_stock_temp_in_date_range(start_date=start_date, end_date=day)
        print(f"获取到临时数据：{len(all_temp_k_info)} 条")
        self.copy_data_to_bs_stock_data_k_from_temp(all_temp_k_info=all_temp_k_info)
        self.connection.commit()
        speed = datetime.now() - start
        print(f"数据复制完毕,耗时： {speed}")
        return self

    def cp_one_from_temp(self, code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        one_temp_k_info = self.get_stock_date_range_k_temp(code=code, start_date=start_date, day=day)
        print(f"获取到临时数据：{len(one_temp_k_info)} 条")
        self.copy_data_to_bs_stock_data_k_from_temp(all_temp_k_info=one_temp_k_info)
        self.connection.commit()
        print(f"数据复制完成")
        return self

    # 将传入数据写入k线表
    def copy_data_to_bs_stock_data_k_from_temp(self, all_temp_k_info):
        for one_temp_k_info in all_temp_k_info:
            insert = dict_to_mysql_insert(table_name="bs_stock_data_day_k", data_dict=one_temp_k_info,
                                          need_camel_to_snake=False)
            # print(insert)
            # 创建一个新的列表，用于存储转换后的值
            values = []
            # 遍历字典的值，并将空字符串转换为None
            for value in one_temp_k_info.values():
                if value == '':
                    values.append(None)  # 空字符串转换为None
                else:
                    values.append(value)  # 其他值保持不变
            self._execute_insert_sql(insert, values)
        return self

    def del_k_data(self, code: str, commit=False):
        del_sql = f"""
            delete from bs_stock_data_day_k where code = '{code}'
        """
        self._execute_delete_sql(del_sql, commit=commit)
        return self

    def clear_k_data_temp(self):
        clear_k_data_temp_sql = """
           delete from bs_stock_data_k_temp  
        """
        self._execute_delete_sql(sql=clear_k_data_temp_sql, commit=True)
        return self
