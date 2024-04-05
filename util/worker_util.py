import pandas as pd


def pd_task_slicing_avg(data: pd.DataFrame, task_count=20) -> dict[int, pd.DataFrame]:
    result = {}
    row_count = data.shape[0]
    # 计算一个线程需要处理的数量，如果取余大于0就直接再加一个线程数
    one_task_count = int(row_count / task_count)
    surplus_count = row_count % task_count
    left = 0
    right = 0
    index = 0
    while right < row_count:
        # left_cache = left
        left = right
        right = (left + one_task_count + (1 if surplus_count > 0 else 0))
        right = right if right <= row_count else row_count
        surplus_count -= 1
        result[index] = data.iloc[left:right]
        index += 1
    return result
