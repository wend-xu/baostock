from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine

from index.rele import Related
from sql_helper.bao_stock_helper import BaoStockHelper


def rele_report(latest_date: str = None, last_x_day: int = 14):
    bsh = BaoStockHelper().conn()
    related = Related().conn()

    latest_date = datetime.now().strftime("%Y-%m-%d") if latest_date is None else latest_date
    table_name = f"rele_temp_{latest_date.replace('-', '')}"
    index_code_list = ['sh.000001', 'sz.399006', 'sz.399106', 'sh.000300']
    all_in_date = bsh.get_all_stock_with_date(latest_date)

    report_list = []
    for stock_code in all_in_date['code']:
        report = related.rele_stock_and_multi_index_as_report(stock_code=stock_code, index_code_list=index_code_list,
                                                              last_x_day=last_x_day,latest_date=latest_date)
        if report is not None:
            report_list.append(report)
    engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
    pd.DataFrame(report_list).to_sql(table_name, engine, if_exists='append', index=False)


if __name__ == '__main__':
    rele_report(latest_date='2024-10-11')
