import sys, os

import pandas as pd
from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API
from mysql_util import *

api = TdxHq_API(heartbeat=True)


def get_available_connect():
    for ele in hq_hosts:
        res = api.connect(ele[1], ele[2])
        if res:
            return (ele[1], ele[2])
    return tuple()


def update_micro_index_data():
    host, port = get_available_connect()
    api.connect(host, port)
    data = api.get_index_bars(9, 1, '880823', 1, 500)
    sql = '''replace into stock.micro_index values (%s, %s, %s, %s, %s, %s)'''
    df = pd.DataFrame(data)
    df1 = df[['datetime', 'open', 'close', 'high', 'low', 'vol']]
    df1['datetime'] = df1['datetime'].map(lambda x: x[:10])
    insert_table_by_batch(sql, df1.values.tolist(), batch_size=100)
    os._exit(0)


if __name__ == '__main__':
    update_micro_index_data()
