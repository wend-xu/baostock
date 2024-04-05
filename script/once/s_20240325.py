from sql_helper.bao_stock_helper import BaoStockHelper
from sql_helper.xq_stock_helper import XqStockHelper
from index.macd import Macd
from datetime import datetime

bsh = BaoStockHelper()
bsh.conn()

xqsh = XqStockHelper()
xqsh.conn()

all_today_df = bsh.get_all_stock_with_date()

gold_cross_list = []

start = datetime.now()
for code in all_today_df['code']:
    print(f"开始处理债券: {code}")
    stock_last_x_day_k_df = bsh.get_stock_last_x_day_k_as_df(code=code, last_x_day=120)
    macd = None
    try:
        macd = Macd(k_data=stock_last_x_day_k_df).dea_s_l_x()
    except Exception as ex:
        print(f"{code} 分析出现异常 ： {str(ex)}")
        continue
    dea = macd.get_dea_s_l_x()
    dif = macd.get_dif_s_l()
    cross = dif - dea
    cross_latest = cross.iloc[0]
    cross_prev = cross.iloc[1]
    if cross_latest > 0 > cross_prev:
        print(f"{code}出现金叉")
        gold_cross_list.append({'code': code, 'latest': cross_latest, 'prev': cross_prev})
    print(f"当前日期共抓取到金叉个股数： {len(gold_cross_list)} ")

    # 进度：{percent:.2f}
for gold_cross in gold_cross_list:
    code = gold_cross['code']
    print(f"债券编码： {code}")
    xq = xqsh.get_stock_latest_info(code[:2].upper() + code[3:])
    print(f"\t债券编码:{code}, 名称：{xq.name} ,总市值{xq.float_market_capital/100000000:.2f} ,主力当日流入：{xq.main_net_inflows} , 北向净流入: {xq.north_net_inflow} ,涨跌幅： {xq.percent} ,当前股价： {xq.current} ,年初至今涨跌幅: {xq.current_year_percent} ，pe_ttm :{xq.pe_ttm}")
bsh.dis_conn()
print(f"总耗时：{datetime.now() - start}")
