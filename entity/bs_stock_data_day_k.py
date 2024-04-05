from dataclasses import dataclass, asdict, fields
from typing import Dict, Any
from datetime import date


# 定义Python实体类
@dataclass
class BsStockDataDayK:
    date: date
    code: str
    open: float = None
    high: float = None
    low: float = None
    close: float = None
    preclose: float = None
    volume: int = None
    amount: float = None
    adjustflag: int = None
    turn: float = None
    tradestatus: int = None
    pctChg: float = None
    peTTM: float = None
    pbMRQ: float = None
    psTTM: float = None
    pcfNcfTTM: float = None
    isST: int = None

    def __init__(self):
        super().__init__()

    # 将实体类转换为字典
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    @staticmethod
    def to_obj(dictionary: Dict):
        bs = BsStockDataDayK()
        for key, value in dictionary.items():
            setattr(bs, key, value)
        return bs
