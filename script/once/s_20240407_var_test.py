import pandas as pd
from sqlalchemy import create_engine

df = pd.read_csv('ols_rolling_window_fitting.csv', sep=',')
engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
df.to_sql('analyse_report_temp', engine, if_exists='append', index=False)
