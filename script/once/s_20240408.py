# code = "sz.300037"
import multiprocessing

import pandas as pd
from sqlalchemy import create_engine

from index.macd import Macd
from index.ols import Ols
from sql_helper.bao_stock_helper import BaoStockHelper
from util.worker_util import multi_process_execute


def handle_section(stock_df_section: pd.DataFrame):
    bsh_in_section = BaoStockHelper().conn()
    result_array = []
    for code in stock_df_section['code']:
        stock_df = bsh_in_section.get_stock_date_in_date_range_as_df(code=code, start_date="2020-01-01",
                                                                     day="2024-04-03")
        macd = Macd(k_data=stock_df)
        macd.get_dif_s_l()
        ols = Ols(data=stock_df, ols_data_key=macd.get_dif_s_l_key())
        slide_fit_result = ols.slide_fit(slide_interval=7, start=0)
        base_stock_date = bsh_in_section.get_base_stock_date(code)
        slide_fit_result = {**base_stock_date, **slide_fit_result}
        print(slide_fit_result)
        result_array.append(slide_fit_result)
    engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
    pd.DataFrame(result_array).to_sql('slide_fit_result_temp', engine, if_exists='append', index=False)
    bsh_in_section.dis_conn()


if __name__ == '__main__':
    bsh = BaoStockHelper().conn()
    all_in_date = bsh.get_all_stock_with_date("2024-04-03")
    multiprocessing.set_start_method('spawn')
    multi_process_execute(data=all_in_date, target=handle_section)
    bsh.dis_conn()
