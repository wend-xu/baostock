import pandas as pd


class IndexCommon:
    def __init__(self):
        super().__init__()

    def pd_df_get_column_if_null_generate(self, df: pd.DataFrame, key: str):
        column = df.get(key)
        if column is None:
            length = df.shape[0]
            column = pd.Series([None] * length)
            df[key] = column
        return df.get(key)

    def pd_df_set_to_column(self, df: pd.DataFrame, key: str, index: int, value):
        self.pd_df_get_column_if_null_generate(df=df, key=key)
        df.at[index, key] = value

    def pd_df_set_to_column_batch(self, df: pd.DataFrame, key: str, index_start: int, index_end: int, value):
        self.pd_df_get_column_if_null_generate(df=df, key=key)
        df.loc[df.index[index_start:index_end], key] = value

    def pd_df_range_sum(self, df: pd.DataFrame, key: str, index_start: int, index_end: int):
        column = df.get(key)
        if column is None:
            print(f"列 {key} 不存在于数据集")
            return 0
        return column.iloc[index_start:index_end].sum()

    def pd_df_range_mean(self, df: pd.DataFrame, key: str, index_start: int, index_end: int):
        column = df.get(key)
        if column is None:
            print(f"列 {key} 不存在于数据集")
            return 0
        return column.iloc[index_start:index_end].mean()
