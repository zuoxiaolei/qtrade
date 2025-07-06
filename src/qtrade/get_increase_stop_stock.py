from datetime import datetime
from loguru import logger
from src.qtrade.mysql_util import *
import schedule
from datetime import time as datetime_time

logger.add("stock.log", rotation="100 MB", compression="zip", mode='w')

STOCK_SQL = """
WITH LatestRecords AS (
	SELECT CODE
		,
		NAME,
		CLOSE,
		now,
		t1.TIMESTAMP,
		( now - CLOSE ) / CLOSE * 100 AS current_change_percent 
	FROM
		stock.stock_price_real_time_{today} t1
		JOIN ( SELECT DISTINCT TIMESTAMP FROM stock.stock_price_real_time_{today} ORDER BY TIMESTAMP DESC LIMIT 30 ) t2 ON t1.TIMESTAMP = t2.`timestamp` 
	WHERE
		CODE NOT IN (
		SELECT CODE 
		FROM
			stock.stock_increase_max t1
			JOIN ( SELECT DISTINCT DATE FROM stock.stock_increase_max ORDER BY DATE DESC LIMIT 30 ) t2 ON t1.DATE = t2.DATE 
		GROUP BY
		code 
		) and
		code NOT LIKE '51%' AND 
    code NOT LIKE '58%' AND
    
    -- 排除指数（通常以000或399开头）
    code NOT LIKE '000%' AND 
    code NOT LIKE '399%' AND
    
    -- 排除创业板（以300开头）
    code NOT LIKE '300%' AND
    
    -- 排除科创板（以688开头）
    code NOT LIKE '688%' AND
    
    -- 排除北交所（以8或43、83、87开头）
    code NOT LIKE '8%' AND 
    code NOT LIKE '43%' AND 
    code NOT LIKE '83%' AND 
    code NOT LIKE '87%'
	),
	MinPriceData AS ( SELECT min( now ) now, CODE FROM LatestRecords GROUP BY CODE ) SELECT
	t1.CODE,
	t1.TIMESTAMP,
	t1.current_change_percent,
	( t1.now - t2.now )/ t2.now * 100 chang_30minutes 
FROM
	LatestRecords t1
	JOIN MinPriceData t2 ON t1.CODE = t2.CODE 
WHERE
	t1.TIMESTAMP IN ( SELECT max( TIMESTAMP ) TIMESTAMP FROM stock.stock_price_real_time_{today} ) 
	AND current_change_percent BETWEEN 7.0 
	AND 9.9 
	AND ( t1.now - t2.now )/ t2.now * 100 > 3
"""

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

def job():
    try:
        if is_stock_trading_time():
            logger.info("当前是交易时间")
            with get_connection() as cursor:
                sql = STOCK_SQL.format(today=datetime.today().strftime("%Y%m%d"))
                cursor.execute(sql)
                result = cursor.fetchall()
                if result:
                    logger.info(f"抓涨停列表为: {result}")
        else:
            logger.info("当前不是交易时间")
    except:
        print("获取股票失败")
        import traceback
        traceback.print_exc()

def get_stock():
    # 每分钟的第59秒执行job函数
    schedule.every().minute.at(":03").do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    get_stock()
    