from typing import List, Union, Dict

import baostock as bs
import mysql.connector
from datetime import datetime

import pandas as pd
from mysql.connector.abstracts import MySQLConnectionAbstract, MySQLCursorAbstract
from mysql.connector.pooling import PooledMySQLConnection
from sqlalchemy import create_engine

from sql_helper.insert_sql_helper import dict_to_mysql_insert


class BaoStockIndexHelper:
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

    def get_index_temp_in_date_range(self, index_code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        get_index_temp_in_date_range_sql = f"""
            select * from bs_index_data_k_temp where code = '{index_code}' and  date between '{start_date}' and '{day}' order by date desc;
        """
        return self._execute_query_sql(get_index_temp_in_date_range_sql)

    def get_index_in_date_range(self, index_code: str, day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        get_index_temp_in_date_range_sql = f"""
            select * from bs_index_data_day_k where code = '{index_code}' and  date between '{start_date}' and '{day}' order by date desc;
        """
        return self._execute_query_sql(get_index_temp_in_date_range_sql)

    def get_index_last_x_day(self, index_code: str, x: int):
        get_index_last_x_day_sql = f"""
            select * from bs_index_data_day_k where code = '{index_code}'  order by date desc limit {x}; 
        """
        return self._execute_query_sql(get_index_last_x_day_sql)

    def get_index_last_x_day_as_df(self, index_code: str, x: int):
        return pd.DataFrame(self.get_index_last_x_day(index_code=index_code, x=x))

    def _init_date_range(self, start_date=None, day=None):
        if day is None or day == "":
            day = datetime.now().strftime("%Y-%m-%d")
        if start_date is None or start_date == "":
            start_date = day
        return start_date, day

    def sync_index_k_day_to_temp(self, index_code_list: List[str], day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
        for index_code in index_code_list:
            rs = bs.query_history_k_data_plus(code=index_code,
                                              fields="date,code,open,high,low,close,preclose,volume,amount,pctChg",
                                              start_date=start_date, end_date=day, frequency="d")
            data = rs.get_data()
            print(f"{index_code} 获取到时间段 {start_date} - {day} 数据共 {data.shape[0]} 条")
            data.to_sql('bs_index_data_k_temp', engine, if_exists='append', index=False)
        return self

    def cp_index_k_day_from_temp(self, index_code_list: List[str], day=None, start_date=None):
        start_date, day = self._init_date_range(start_date=start_date, day=day)
        for index_code in index_code_list:
            temp_data_in_date_range = self.get_index_temp_in_date_range(index_code=index_code, start_date=start_date,
                                                                        day=day)
            print(f"{index_code} 获取到临时数据 {len(temp_data_in_date_range)} 条")
            for temp_data in temp_data_in_date_range:
                insert = dict_to_mysql_insert(table_name="bs_index_data_day_k", data_dict=temp_data,
                                              need_camel_to_snake=False)
                values = []
                # 遍历字典的值，并将空字符串转换为None
                for value in temp_data.values():
                    if value == '':
                        values.append(None)  # 空字符串转换为None
                    else:
                        values.append(value)  # 其他值保持不变
                self._execute_insert_sql(insert, values)
        return self

    def clear_temp_index_data(self):
        clear_temp_index_data_sql = """
            delete from bs_index_data_k_temp;
        """
        self._execute_delete_sql(sql=clear_temp_index_data_sql, commit=True)
        return self
