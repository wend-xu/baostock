from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine

from index.macd import Macd
from index.ols import Ols
from sql_helper.bao_stock_helper import BaoStockHelper
from sql_helper.xq_stock_helper import XqStockHelper
from util.worker_util import multi_process_execute, log_err


def handle_one(bsh: BaoStockHelper, xqh: XqStockHelper, code: str, ipo_date_max: date, fit_start_date: str,
               fit_oldest_date="2020-01-01"):
    # io瓶颈，可以通过将数据丢到redis，减少数据库查询操作
    # 在单进程执行完成大约需要20分钟，纯粹的数据库读操作大概一分半
    # 在多进程执行时，2分钟可以完成全部，数据库操作可以进一步优化执行速度
    base_stock_data = bsh.get_base_stock_date(code)
    # todo 这里的忽略条件应该调整为动态的，根据 fit_start_date 和 slide_interval 计算出一次拟合需要的最少数据
    # 理论上macd的 长周期、短周期、eda均线周期、滑动拟合周期都应该支持传入
    if base_stock_data.get('ipoDate') is None or base_stock_data['ipoDate'] > ipo_date_max:
        print(f"忽略{ipo_date_max}后上市个股")
        log_err(f"{code} 的上市时间为 [{base_stock_data.get('ipoDate')}],忽略")
        return None
    stock_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date=fit_oldest_date,
                                                      day=fit_start_date)
    # 计算瓶颈，macd指标可以增量计算入库，最新数据只受上一个数据影响
    macd = Macd(k_data=stock_df)
    macd.get_dif_s_l()
    # 计算瓶颈，持续拟合计算是消耗性能的元凶，使用跳步的方式，可以加速性能
    ols = Ols(data=stock_df, ols_data_key=macd.get_dif_s_l_key())
    slide_fit_result = ols.slide_fit(slide_interval=7, start=0)
    pct_chg_analyse = bsh.analyse_pct_chg(data=stock_df, start=slide_fit_result['start'],
                                          interval=slide_fit_result['fit_range_interval'])
    xq_analyse = xqh.analyse_stock_in_interval(symbol=XqStockHelper.bs_code_2_xq_symbol(code),
                                               start_date=fit_start_date,
                                               interval=slide_fit_result['fit_range_interval'])
    slide_fit_result = {**base_stock_data, **pct_chg_analyse, **slide_fit_result, **xq_analyse}
    print(slide_fit_result)
    return slide_fit_result


def handle_section(index: int, stock_df_section: pd.DataFrame, *args, **kwargs):
    fit_start_date = kwargs.get("start_date")
    table_name = kwargs.get("table_name")
    if table_name is None:
        table_name = f"slide_fit_result_temp_{fit_start_date.replace('-', '')}"
    fit_oldest_date = "2020-01-01"

    print(
        f"当前进程：{index} 开始执行拟合切片，切片任务数: {stock_df_section.shape[0]} ,拟合数据起止点 {fit_oldest_date} - {fit_start_date} ,结果存储表名： {table_name}")

    bsh_in_section = BaoStockHelper().conn()
    xqh_in_section = XqStockHelper().conn()
    ipo_date_max = date(2023, 1, 1)
    result_array = []
    for code in stock_df_section['code']:
        try:
            slide_fit_result = handle_one(bsh_in_section, xqh_in_section, code, ipo_date_max,
                                          fit_start_date=fit_start_date,
                                          fit_oldest_date=fit_oldest_date)
            if slide_fit_result is not None:
                result_array.append(slide_fit_result)
        except Exception as ex:
            log_err(f"{code} 执行出现错误: {str(ex)}")
    engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
    pd.DataFrame(result_array).to_sql(table_name, engine, if_exists='append', index=False)
    bsh_in_section.dis_conn()
    xqh_in_section.dis_conn()


def stock_slide_fit_report_multiprocess(fit_start_date: str = None):
    fit_start_date = datetime.now().strftime("%Y-%m-%d") if fit_start_date is None else fit_start_date
    bsh = BaoStockHelper().conn()
    all_in_date = bsh.get_all_stock_with_date(fit_start_date)
    multi_process_execute(data=all_in_date, target=handle_section, start_date=fit_start_date)
    bsh.dis_conn()


if __name__ == '__main__':
    stock_slide_fit_report_multiprocess(fit_start_date="2024-04-03")
