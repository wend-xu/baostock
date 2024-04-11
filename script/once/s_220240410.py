from sql_helper.bao_stock_helper import BaoStockHelper

code, start_date, day = "603083", "2020-01-01", "2024-04-03"
bsh = BaoStockHelper().conn()
bash_data = bsh.get_base_stock_date(code="sz.002085")
df = bsh.get_stock_last_x_day_k_as_df(code="sz.002085", last_x_day=120)
print({**bash_data, **bsh.analyse_pct_chg(df, 0, 30)})
bsh.dis_conn()
