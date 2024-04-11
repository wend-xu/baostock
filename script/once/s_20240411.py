from index.macd import Macd
from index.ols import Ols
from sql_helper.bao_stock_helper import BaoStockHelper

code, start_date, day = "sz.000725", "2020-01-01", "2024-04-03"
bsh = BaoStockHelper().conn()
stock_df = bsh.get_stock_date_in_date_range_as_df(code=code, start_date=start_date,
                                                  day=day)
base_stock_data = bsh.get_base_stock_date(code)
# 计算瓶颈，macd指标可以增量计算入库，最新数据只受上一个数据影响
macd = Macd(k_data=stock_df)
macd.get_dif_s_l()
# 计算瓶颈，持续拟合计算是消耗性能的元凶，使用跳步的方式，可以加速性能
ols = Ols(data=stock_df, ols_data_key=macd.get_dif_s_l_key())
slide_fit_result = ols.slide_fit(slide_interval=7, start=0, stop_condition=lambda slope_fit: slope_fit < 0.01)
pct_chg_analyse = bsh.analyse_pct_chg(data=stock_df, start=slide_fit_result['start'],
                                      interval=slide_fit_result['fit_range_interval'])
slide_fit_result = {**slide_fit_result, **pct_chg_analyse, **base_stock_data, }
print(slide_fit_result)
ols.plot()
