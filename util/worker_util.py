import multiprocessing
import os
import pickle
import time
from datetime import datetime
from multiprocessing import Queue
from typing import Callable

import pandas as pd


def pd_task_slicing_mean(data: pd.DataFrame, task_count=20) -> dict[int, pd.DataFrame]:
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


def log_err(message: str):
    with open('error.txt', 'w', encoding='utf-8') as file:
        # 写入一些文本
        file.write(f'{datetime.now()} - {message} \n')


def multi_process_execute_target(index, pickle_dump, target: Callable[[pd.DataFrame], None]):
    try:
        print(f"进程{index} 开始执行")
        if pickle_dump is not None:
            data_df = pickle.loads(pickle_dump)
            target(data_df)
            print(f"进程 {index} 执行完成")
        else:
            print(f"进程 {index} 数据为空")
    except Exception as ex:
        log_err(f"进程{index} 出现异常：\n {str(ex)}")



def multi_process_execute(data: pd.DataFrame, target: Callable[[pd.DataFrame], None], max_process_count=None):
    start = datetime.now()
    multiprocessing.set_start_method('spawn')
    max_process_count = os.cpu_count() if max_process_count is None else max_process_count
    print(f"开始执行任务:{start} ,最大进程数:{max_process_count}")
    slicing_result = pd_task_slicing_mean(data=data, task_count=max_process_count)
    process_array = []
    for index in slicing_result:
        data = slicing_result[index]
        print(f"准备启动进程 {index} ,当前进程分配任务数: {data.shape[0]}")
        dump = pickle.dumps(data)
        process = multiprocessing.Process(target=multi_process_execute_target, args=(index, dump, target))
        process.start()
        print(f"线程启动完成")
        process_array.append(process)

    for process in process_array:
        process.join()

    end = datetime.now()
    print(f"完成任务执行:{end} ,耗时： {end - start}")
