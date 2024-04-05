import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict, fields


@dataclass
class XqStockDataDay:
    # def __init__(self,
    #              symbol: str,
    #              date: datetime.date,
    #              north_net_inflow: Optional[float] = None,
    #              ps: Optional[float] = None,
    #              type: Optional[int] = None,
    #              percent: Optional[float] = None,
    #              has_follow: Optional[int] = None,
    #              tick_size: Optional[float] = None,
    #              pb_ttm: Optional[float] = None,
    #              float_shares: Optional[int] = None,
    #              current: Optional[float] = None,
    #              amplitude: Optional[float] = None,
    #              pcf: Optional[float] = None,
    #              current_year_percent: Optional[float] = None,
    #              float_market_capital: Optional[float] = None,
    #              north_net_inflow_time: Optional[int] = None,
    #              market_capital: Optional[int] = None,
    #              dividend_yield: Optional[float] = None,
    #              lot_size: Optional[int] = None,
    #              roe_ttm: Optional[float] = None,
    #              total_percent: Optional[float] = None,
    #              percent5m: Optional[float] = None,
    #              income_cagr: Optional[float] = None,
    #              amount: Optional[float] = None,
    #              chg: Optional[float] = None,
    #              issue_date_ts: Optional[int] = None,
    #              eps: Optional[float] = None,
    #              main_net_inflows: Optional[int] = None,
    #              volume: Optional[int] = None,
    #              volume_ratio: Optional[float] = None,
    #              pb: Optional[float] = None,
    #              followers: Optional[int] = None,
    #              turnover_rate: Optional[float] = None,
    #              first_percent: Optional[float] = None,
    #              name: Optional[str] = None,
    #              pe_ttm: Optional[float] = None,
    #              total_shares: Optional[int] = None,
    #              limitup_days: Optional[int] = None):
    #     self.symbol = symbol
    #     self.date = date
    #     self.north_net_inflow = north_net_inflow
    #     self.ps = ps
    #     self.type = type
    #     self.percent = percent
    #     self.has_follow = has_follow
    #     self.tick_size = tick_size
    #     self.pb_ttm = pb_ttm
    #     self.float_shares = float_shares
    #     self.current = current
    #     self.amplitude = amplitude
    #     self.pcf = pcf
    #     self.current_year_percent = current_year_percent
    #     self.float_market_capital = float_market_capital
    #     self.north_net_inflow_time = north_net_inflow_time
    #     self.market_capital = market_capital
    #     self.dividend_yield = dividend_yield
    #     self.lot_size = lot_size
    #     self.roe_ttm = roe_ttm
    #     self.total_percent = total_percent
    #     self.percent5m = percent5m
    #     self.income_cagr = income_cagr
    #     self.amount = amount
    #     self.chg = chg
    #     self.issue_date_ts = issue_date_ts
    #     self.eps = eps
    #     self.main_net_inflows = main_net_inflows
    #     self.volume = volume
    #     self.volume_ratio = volume_ratio
    #     self.pb = pb
    #     self.followers = followers
    #     self.turnover_rate = turnover_rate
    #     self.first_percent = first_percent
    #     self.name = name
    #     self.pe_ttm = pe_ttm
    #     self.total_shares = total_shares
    #     self.limitup_days = limitup_days
    symbol: str  # 股票代码
    date: str  # 日期
    north_net_inflow: float = 0  # 北向资金净流入
    ps: float = 0  # 市销率
    type: int = 0  # 股票类型
    percent: float = 0  # 涨跌幅
    has_follow: bool = 0  # 是否有关注
    tick_size: float = 0  # 最小变动价位
    pb_ttm: float = 0  # 市净率（TTM）
    float_shares: int = 0  # 流通股本
    current: float = 0  # 当前价格
    amplitude: float = 0  # 振幅
    pcf: float = 0  # 市现率
    current_year_percent: float = 0  # 年初至今涨跌幅
    float_market_capital: float = 0  # 流通市值
    north_net_inflow_time: int = 0  # 北向资金净流入时间
    market_capital: int = 0  # 总市值
    dividend_yield: float = 0  # 股息率
    lot_size: int = 0  # 每手股数
    roe_ttm: float = 0  # 净资产收益率（TTM）
    total_percent: float = 0  # 上市至今涨跌幅
    percent5m: float = 0  # 5分钟涨跌幅
    income_cagr: float = 0  # 收入复合增长率
    amount: float = 0  # 成交额
    chg: float = 0  # 涨跌额
    issue_date_ts: int = 0  # 上市日期
    eps: float = 0  # 每股收益
    main_net_inflows: int = 0  # 主力净流入
    volume: int = 0  # 成交量
    volume_ratio: float = 0  # 量比
    pb: float = 0  # 市净率
    followers: int = 0  # 关注者数
    turnover_rate: float = 0  # 换手率
    first_percent: float = 0  # 首日涨跌幅
    name: str = 0  # 股票名称
    pe_ttm: float = 0  # 市盈率（TTM）
    total_shares: int = 0  # 总股本
    limitup_days: int = 0  # 连续涨停天数

    def __init__(self):
        super().__init__()

    # 这里可以添加更多方法，例如数据验证、计算属性等

    # 将实体类转换为字典
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

        # 从字典恢复实体类

    @staticmethod
    def to_obj(dictionary: Dict):
        bs = XqStockDataDay()
        for key, value in dictionary.items():
            if value is not None:
                setattr(bs, key, value)
        return bs
