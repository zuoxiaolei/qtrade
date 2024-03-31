import time

import pymysql
import os
from contextlib import contextmanager
from tqdm import tqdm

host = os.environ['MYSQL_IP']
port = os.environ['MYSQL_PORT']
user = os.environ['MYSQL_USER']
password = os.environ['MYSQL_PASSWORD']
database = 'etf'
thread_num = 10


def get_mysql_connection(database):
    conn = pymysql.connect(
        host=host,  # 主机名
        port=int(port),  # 端口号，MySQL默认为3306
        user=user,  # 用户名
        password=password,  # 密码
        database=database,  # 数据库名称
        charset='utf8mb4'
    )
    return conn


@contextmanager
def get_connection():
    conn = get_mysql_connection('etf')
    cursor = conn.cursor()
    yield cursor
    conn.commit()
    cursor.close()
    conn.close()


def time_cost(func):
    def deco(*args, **kwargs):
        start_time = time.time()
        res = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} use {end_time - start_time}second")
        return res

    return deco


def get_max_date(n=1):
    with get_connection() as cursor:
        sql = f'''select date from dim_etf_trade_date order by date desc limit {n-1},1'''
        cursor.execute(sql)
        res = cursor.fetchall()
        date = res[0][0]
    return date


def split_data(data, batch_size):
    for i in range(0, len(data), batch_size):
        yield data[i:(i+batch_size)]
    

@time_cost
def insert_table_by_batch(sql, data, batch_size=100):
    if not data:
        return
    batch_data = list(split_data(data, batch_size))
    print(sql)
    for ele in tqdm(batch_data):
        with get_connection() as cursor:
            cursor.executemany(sql, ele)
