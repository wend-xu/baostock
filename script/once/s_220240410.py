# import pandas as pd
# from sqlalchemy import create_engine
#
# from sql_helper.bao_stock_helper import BaoStockHelper
# from sql_helper.xq_stock_helper import XqStockHelper
#
# code, start_date, day = "sz.000002", "2020-01-01", "2024-04-03"
# # bsh = BaoStockHelper().conn()
# # bash_data = bsh.get_base_stock_date(code="sz.002085")
# # df = bsh.get_stock_last_x_day_k_as_df(code="sz.002085", last_x_day=120)
# # print({**bash_data, **bsh.analyse_pct_chg(df, 0, 30)})
# # bsh.dis_conn()
#
# xqh = XqStockHelper().conn()
# analyse = xqh.analyse_stock_in_interval(symbol=XqStockHelper.bs_code_2_xq_symbol(code),interval=14)
# print(analyse)
# df = pd.DataFrame([analyse])
# engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
# df.to_sql("test1", engine, if_exists='append', index=False)
# xqh.dis_conn()