# code = "sz.300037"
import multiprocessing
from datetime import date

import pandas as pd
from sqlalchemy import create_engine

from index.macd import Macd
from index.ols import Ols
from sql_helper.bao_stock_helper import BaoStockHelper
from util.worker_util import multi_process_execute, log_err


def handle_one(bsh: BaoStockHelper, code: str, ipo_date_max):
    base_stock_data = bsh.get_base_stock_date(code)
    if base_stock_data.get('ipoDate') is None or base_stock_data['ipoDate'] > ipo_date_max:
        print("忽略2023年后上市个股")
        log_err(f"{code} 的上市时间为 [{base_stock_data.get('ipoDate')}],忽略")
        return None
    stock_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date="2020-01-01",
                                                      day="2024-04-03")
    macd = Macd(k_data=stock_df)
    macd.get_dif_s_l()
    ols = Ols(data=stock_df, ols_data_key=macd.get_dif_s_l_key())
    slide_fit_result = ols.slide_fit(slide_interval=7, start=0)
    slide_fit_result = {**base_stock_data, **slide_fit_result}
    print(slide_fit_result)
    return slide_fit_result


def handle_section(stock_df_section: pd.DataFrame):
    bsh_in_section = BaoStockHelper().conn()
    ipo_date_max = date(2023, 1, 1)
    result_array = []
    for code in stock_df_section['code']:
        try:
            slide_fit_result = handle_one(bsh_in_section, code, ipo_date_max)
            if slide_fit_result is not None:
                result_array.append(slide_fit_result)
        except Exception as ex:
            log_err(f"{code} 执行出现错误: {str(ex)}")
        # base_stock_data = bsh.get_base_stock_date(code)
        # if base_stock_data.get('ipoDate') is None or base_stock_data['ipoDate'] > ipo_date_max:
        #     print("忽略2023年后上市个股")
        #     continue
        # stock_df = bsh_in_section.get_stock_date_in_date_range_as_df(code=code, start_date="2020-01-01",
        #                                                              day="2024-04-03")
        # macd = Macd(k_data=stock_df)
        # macd.get_dif_s_l()
        # ols = Ols(data=stock_df, ols_data_key=macd.get_dif_s_l_key())
        # slide_fit_result = ols.slide_fit(slide_interval=7, start=0)
        # slide_fit_result = {**base_stock_data, **slide_fit_result}
        # print(slide_fit_result)
        # result_array.append(slide_fit_result)
    engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
    pd.DataFrame(result_array).to_sql('slide_fit_result_temp', engine, if_exists='append', index=False)
    bsh_in_section.dis_conn()


if __name__ == '__main__':
    bsh = BaoStockHelper().conn()
    all_in_date = bsh.get_all_stock_with_date("2024-04-03")
    multi_process_execute(data=all_in_date, target=handle_section)
    bsh.dis_conn()
