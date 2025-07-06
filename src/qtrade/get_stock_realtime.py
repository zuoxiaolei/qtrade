
import akshare as ak
import time
import easyquotation
import pandas as pd
from src.qtrade.mysql_util import *
from datetime import datetime
from loguru import logger
import schedule
from datetime import time as datetime_time
from datetime import timedelta

quotation = easyquotation.use('sina')


def get_from_akshare():
    # 60秒
    start_time = time.time()
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    end_time = time.time()
    print(end_time-start_time)
    print(stock_zh_a_spot_em_df)
    print(stock_zh_a_spot_em_df.shape)




def delete_old_table():
    """删除旧表"""
    today = datetime.today().strftime("%Y%m%d")
    select_all_tables_sql = """SELECT
        table_name 
    FROM
        information_schema.TABLES 
    WHERE
        table_schema = 'stock'
    """
    with get_connection() as cursor:
        cursor.execute(select_all_tables_sql)
        result = cursor.fetchall()
    tablenames = [ele[0] for ele in result if ele[0].startswith("stock_price_real_time_")]
    keep_table = f"stock_price_real_time_{today}"
    logger.info(f"删除旧表{tablenames}，保留{keep_table}")
    for ele in tablenames:
        if ele != keep_table:
            sql = f"DROP TABLE IF EXISTS stock.{ele}"
            with get_connection() as cursor:
                cursor.execute(sql)
                logger.info(f"删除表{ele}成功!")

def create_table_if_not_exist():
    today = datetime.today().strftime("%Y%m%d")
    """创建表"""
    sql = f"""CREATE TABLE IF NOT EXISTS stock.stock_price_real_time_{today} (
            `date` varchar(20) DEFAULT NULL,
            `timestamp` bigint NOT NULL,
            `code` varchar(255) NOT NULL,
            `name` varchar(255) DEFAULT NULL,
            `open` double(255,0) DEFAULT NULL,
            `close` double(255,0) DEFAULT NULL,
            `now` double(255,0) DEFAULT NULL,
            `high` double(255,0) DEFAULT NULL,
            `low` double DEFAULT NULL,
              PRIMARY KEY (`timestamp`,`code`),
            KEY `code_idx` (`code`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
            """
    with get_connection() as cursor:
        cursor.execute(sql)
        logger.info(f"stock.stock_price_real_time_{today} 创建表成功!")


def update_from_easyquotation():
    """获取实时数据"""
    data = []
    ea = quotation.market_snapshot(prefix=False)
    for k, v in ea.items():
        v["code"] = k
        data.append(v)
    df = pd.DataFrame(data)
    df = df[["date", "code", "name", "open", "close", "now", "high", "low"]]
    df["timestamp"] = int(time.time())
    df = df[["date", "timestamp", "code", "name", "open", "close", "now", "high", "low"]]
    delete_old_table()
    create_table_if_not_exist()
    today = datetime.today().strftime("%Y%m%d")
    sql = f"""
    INSERT INTO stock.stock_price_real_time_{today} (
        `date`,
        `timestamp`,
        `code`,
        `name`,
        `open`,
        `close`,
        `now`,
        `high`,
        `low`
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    insert_table_by_batch(sql, df.values.tolist(), batch_size=10000)



def job():
    # 获取当前时间并打印，用于验证
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    is_trade_time = is_stock_trading_time()
    if is_trade_time:
        update_from_easyquotation()
    logger.info(f"任务执行于: {current_time} is_trade_time: {is_trade_time}")



def get_increase_max():
    """获取涨停股票"""
    now = datetime.now()
    old_datetime = now-timedelta(days=7)
    while old_datetime<=now:
        date_str = old_datetime.strftime("%Y%m%d")
        stock_zt_pool_em_df = ak.stock_zt_pool_em(date=date_str)
        if stock_zt_pool_em_df.empty:
            old_datetime = old_datetime+timedelta(days=1)
            continue
        stock_zt_pool_em_df = stock_zt_pool_em_df[["代码", "名称"]]
        stock_zt_pool_em_df["date"] = old_datetime.strftime("%Y%m%d")
        stock_zt_pool_em_df["date"] = old_datetime.strftime("%Y%m%d")
        old_datetime = old_datetime+timedelta(days=1)
        stock_zt_pool_em_df.columns = ["code", "name", "date"]
        sql="""
        replace into stock.stock_increase_max VALUES (%s, %s, %s)
        """
        insert_table_by_batch(sql, stock_zt_pool_em_df.values.tolist(), batch_size=1000)
    logger.info(f"获取涨停股票{now}完成")

def is_stock_trading_time():
    now = datetime.now()
    
    # 检查星期几（0-6，0是周一，6是周日）
    if now.weekday() >= 5:  # 5和6是周六和周日
        return False
    
    # 检查时间是否在交易时间段内
    current_time = now.time()
    morning_open = datetime_time(9, 30)
    afternoon_close = datetime_time(15, 30)
    return morning_open <= current_time <= afternoon_close


def run():
    # 每分钟的第59秒执行job函数
    schedule.every().minute.at(":59").do(job)
    schedule.every(1).hour.do(get_increase_max)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    run()