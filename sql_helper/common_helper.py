import pandas as pd


class CommonHelper:
    def __init__(self):
        super().__init__()

    def xq_symbol_2_bs_key(symbol: str) -> str:
        return symbol[0:2].lower() + '.' + symbol[2:]

    def bs_code_2_xq_symbol(code) -> str:
        return code[0:2].upper() + code[3:]

    def sum_series(self, data: pd.Series, length: int):
        return float(data.iloc[0:length].apply(lambda x: 0 if x is None else x).sum()) if len(data) > length else None
