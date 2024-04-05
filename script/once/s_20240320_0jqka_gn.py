import json
import time

from bs4 import BeautifulSoup
import requests
import pandas as pd
from sqlalchemy import create_engine


def do_req(url: str, headers: dict):
    resp = requests.get(url, headers=headers)
    html_content = resp.text
    print(html_content)
    soup = BeautifulSoup(html_content, 'lxml')
    return soup


def get_tr(url: str, headers: dict):
    max_try = 5
    now_try = 0
    tr_all = None
    while now_try < max_try:
        try:
            soup = do_req(url=url, headers=headers)
            tr_all = soup.find('tbody').find_all("tr")
            break
        except Exception:
            now_try = now_try + 1
            print(f"第{now_try}重试失败")
            time.sleep(1)
    if tr_all is None:
        raise Exception("超过最大充实次数获取失败")
    return tr_all


def get_one_page(page: int, headers: dict):
    url = f"https://q.10jqka.com.cn/gn/index/field/addtime/order/desc/page/{page}/ajax/1/"
    tr_all = get_tr(url=url, headers=headers)
    concept_dict = {}
    for tr_one in tr_all:
        td = tr_one.find_all_next("td")
        date = td[0].text
        url = td[1].find("a").attrs['href']
        name = td[1].find("a").text
        code = url.split('/code/')[1].rstrip('/')
        count = td[4].text
        concept = {
            'code': code,
            'date': date,
            'name': name,
            'url': url,
            'count': count
        }
        concept_dict[code] = concept
    return concept_dict


page = 41
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Cookie': 'Hm_lvt_78c58f01938e4d85eaf619eae71b4ed1=1708872088,1709280569,1710918636; Hm_lvt_722143063e4892925903024537075d0d=1708872217,1710919174; Hm_lvt_929f8b362150b1f77b477230541dbbc2=1708872217,1710919174; v=A6MPNsq1wl4_845EXKVWu_zANOxImDeQcSx7GNUA_4J5FM2SXWjHKoH8C3zm',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "Host": "q.10jqka.com.cn",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}

concept_dict_all = {}
for i in range(7, page + 1, 1):
    print(f"开始获取第 {i} 页")
    try:
        concept_dict_page = get_one_page(i, headers)
    except Exception:
        break
    concept_dict_all = {**concept_dict_all, **concept_dict_page}
    print(f"完成获取第 {i} 页")
    time.sleep(1)

print(json.dumps(concept_dict_all))
engine = create_engine("mysql+mysqlconnector://root:qqaazz321@127.0.0.1/stock")
pd.DataFrame(concept_dict_all).to_sql('ths_concept_temp', engine, if_exists='append', index=False)
