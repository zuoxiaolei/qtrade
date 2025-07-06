
import akshare as ak
import time
import easyquotation
import pandas as pd
import schedule

quotation = easyquotation.use('sina')


def get_from_akshare():
    # 60秒
    start_time = time.time()
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    end_time = time.time()
    print(end_time-start_time)
    print(stock_zh_a_spot_em_df)
    print(stock_zh_a_spot_em_df.shape)



def create_table_if_not_exist():
    pass

def get_from_easyquotation():
    """获取实时数据"""
    data = []
    ea = quotation.market_snapshot(prefix=False)
    for k, v in ea.items():
        v["code"] = k
        data.append(v)
    df = pd.DataFrame(data)
    df = df[["date", "code", "name", "open", "close", "now", "high", "low"]]
    print(df)
    print(df.shape)
    print(df.dtypes)

start_time = time.time()
get_from_easyquotation()
end_time = time.time()
print(end_time-start_time)

