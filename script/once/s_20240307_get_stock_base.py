import baostock as bs
import pandas as pd
from sqlalchemy import create_engine

bs.login()
engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
all_stock = bs.query_stock_industry()
data_list = []
while (all_stock.error_code == '0') & all_stock.next():
    # 获取一条记录，将记录合并在一起
    data_list.append(all_stock.get_row_data())
result = pd.DataFrame(data_list, columns=all_stock.fields)
result.to_sql('stock_industry_temp', engine, if_exists='append', index=False)
bs.logout()
