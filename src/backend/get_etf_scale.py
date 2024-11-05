import re
from concurrent.futures import ThreadPoolExecutor

import akshare as ak
import pandas as pd
import requests
import retrying
import tqdm
import json
from mysql_util import get_connection

thread_num = 10
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}


@retrying.retry(stop_max_attempt_number=10, stop_max_delay=10000)
def get_fund_scale(code="159819"):
    url = f"https://fund.eastmoney.com/{code}.html"
    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding
    scale = re.findall("基金规模</a>：(.*?)亿元", resp.text)[0].strip()
    return code, float(scale)


def get_fund_scale2(code="159819"):
    try:
        return get_fund_scale(code)
    except:
        None


def get_all_fund_scale():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    codes = fund_etf_fund_daily_em_df["基金代码"].tolist()
    with ThreadPoolExecutor(thread_num) as executor:
        fund_scale = list(tqdm.tqdm(executor.map(get_fund_scale2, codes), total=len(codes)))
    fund_scale = [ele for ele in fund_scale if ele]
    fund_scale = pd.DataFrame(fund_scale, columns=['code', 'scale'])
    return fund_scale


def stock_em_hsgt_north_net_flow_in(indicator="沪股通"):
    url = "http://push2his.eastmoney.com/api/qt/kamt.kline/get"
    params = {
        "fields1": "f1,f3,f5",
        "fields2": "f51,f52",
        "klt": "101",
        "lmt": "500",
        "ut": "b2884a393a59ad64002292a3e90d46a5",
        "cb": "jQuery18305732402561585701_1584961751919",
        "_": "1584962164273",
    }
    r = requests.get(url, params=params)
    data_text = r.text
    data_json = json.loads(data_text[data_text.find("{"):-2])
    if indicator == "沪股通":
        temp_df = pd.DataFrame(data_json["data"]["hk2sh"]).iloc[:, 0].str.split(",", expand=True)
        temp_df.columns = ["date", "value"]
        return temp_df
    if indicator == "深股通":
        temp_df = pd.DataFrame(data_json["data"]["hk2sz"]).iloc[:, 0].str.split(",", expand=True)
        temp_df.columns = ["date", "value"]
        return temp_df
    if indicator == "北上":
        temp_df = pd.DataFrame(data_json["data"]["s2n"]).iloc[:, 0].str.split(",", expand=True)
        temp_df.columns = ["date", "value"]
        return temp_df


def save_north_flowin():
    df = stock_em_hsgt_north_net_flow_in("北上")
    df["value"] = df["value"].astype(float)
    df["value"] = df["value"] / 10000
    sql = '''
        replace into stock.ods_stock_north_flowin
        values (%s, %s)
        '''
    with get_connection() as cursor:
        cursor.executemany(sql, df[['date', 'value']].values.tolist())
