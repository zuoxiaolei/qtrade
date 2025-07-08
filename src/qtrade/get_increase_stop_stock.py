from datetime import datetime
from loguru import logger
from src.qtrade.mysql_util import *
import schedule
from datetime import time as datetime_time
import random
from xtquant.xttrader import XtQuantTrader
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from xtquant import xtdata
from loguru import logger
import time
from datetime import datetime
from diskcache import Cache
from pathlib import Path
import easyquotation
from easyquotation.helpers import get_stock_type
from diskcache import Cache

BASE_PATH = Path(__file__).absolute().parent
CACHE_PATH = BASE_PATH
cache_object = Cache(str(CACHE_PATH))
cache_object["cache"] = {}
if "max_trade_times" not in cache_object:
    cache_object["max_trade_times"] = 0

quotation = easyquotation.use("sina")
logger.add("stock.log", rotation="100 MB", compression="zip", mode='w')
session_id = int(random.randint(100000, 999999))
mini_qmt_path = r'C:\国金QMT交易端模拟\userdata_mini'
account_id = '55013048'


STOCK_SQL = """
WITH LatestRecords AS (
	SELECT CODE
		,
		NAME,
		CLOSE,
		now,
		t1.TIMESTAMP,
		( now - close ) / close * 100 AS current_change_percent 
	FROM
		stock.stock_price_real_time_{today} t1
		JOIN ( SELECT DISTINCT TIMESTAMP FROM stock.stock_price_real_time_{today} ORDER BY TIMESTAMP DESC LIMIT 30 ) t2 ON t1.TIMESTAMP = t2.`timestamp` 
	WHERE
		CODE NOT IN (
		SELECT CODE 
		FROM
			stock.stock_increase_max t1
			JOIN ( SELECT DISTINCT DATE FROM stock.stock_increase_max ORDER BY DATE DESC LIMIT 20 ) t2 ON t1.DATE = t2.DATE 
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
	AND current_change_percent >= 9.9 
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
    morning_close = datetime_time(11, 30)
    afternoon_open = datetime_time(13, 0)
    afternoon_close = datetime_time(15, 0)
    
    # 上午交易时段或下午交易时段
    return (morning_open <= current_time <= morning_close) or \
           (afternoon_open <= current_time <= afternoon_close)


def get_increase_rate(code):
    # 获取全量tick数据
    stock_data = xtdata.get_full_tick([code])
    current_data = stock_data[code]

    last_price = current_data['lastPrice']  # 最新价
    pre_close = current_data['lastClose']   # 昨收盘价
    change_percent = (last_price - pre_close) / pre_close * 100
    return change_percent

class AutoTrader(object):

    def __init__(self):
        self.xt_trader = XtQuantTrader(mini_qmt_path, session_id)
        self.xt_trader.start()
        connect_result = self.xt_trader.connect()
        self.account = StockAccount(account_id)
        account_subscribe = self.xt_trader.subscribe(self.account)
        total = self.xt_trader.query_stock_asset(self.account).total_asset
        logger.info({"connect_result": connect_result,
                     "account_subscribe": account_subscribe,
                     "total": total})
        self.weight = {'518880'}

    def test_get_real_data(self):
        for code in self.weight:
            code = self.format_code(code)
            stock_data = xtdata.get_full_tick([code])
            logger.info("get stock data "+str({"code": code, "stock_data":stock_data}))
            logger.info(f"get increase rate {get_increase_rate(code)}")

    def is_trade_date(self):
        now = datetime.now()
        now = now.strftime("%Y-%m-%d")
        index_data = quotation.stocks(['512880'])
        trade_date = index_data['512880']["date"]
        logger.info({"trade_date": trade_date, "now": now})
        if now == trade_date:
            return True
        else:
            return False

    def buy_stock(self, stock_code, available_cash):
        stock_data = xtdata.get_full_tick([stock_code])
        logger.info({"stock_data": stock_data})
        avai_v = int(available_cash / stock_data[stock_code]['lastPrice'])
        askPrice = [ele for ele in stock_data[stock_code]["askPrice"] if 0<ele<1e8]
        bidPrice = [ele for ele in stock_data[stock_code]["bidPrice"] if 0<ele<1e8]
        ask_price_2 = bidPrice[0]
        
        avai_v = round(avai_v / 100) * 100
        if avai_v >= 100 and ask_price_2 > 0:
            order_id = self.xt_trader.order_stock(self.account,
                                                  stock_code=stock_code,
                                                  order_type=xtconstant.STOCK_BUY,
                                                  order_volume=avai_v,
                                                  price_type=xtconstant.FIX_PRICE,
                                                  price=ask_price_2)
            logger.info({"buy_stock order_id": order_id, "order_volume": avai_v, "stock_code": stock_code, "ask_price_2": ask_price_2})
            time.sleep(3)
            return {"buy_stock order_id": order_id, "order_volume": avai_v, "stock_code": stock_code}

    def sell_stock(self, stock_code):
        positions = self.xt_trader.query_stock_positions(self.account)
        avail_sell = {}
        for position in positions:
            code = position.stock_code
            can_use_volume = position.can_use_volume
            avail_sell[code] = can_use_volume
        logger.info({"avail_sell": avail_sell})

        if stock_code in avail_sell.keys():
            stock_data = xtdata.get_full_tick([stock_code])
            bidPrice = [ele for ele in stock_data[stock_code]["bidPrice"] if 0<ele<1e8]
            bid_price_2 = bidPrice[-1]

            avai_v = avail_sell[stock_code]
            order_id = self.xt_trader.order_stock(self.account, stock_code=stock_code,
                                                  order_type=xtconstant.STOCK_SELL,
                                                  order_volume=avai_v,
                                                  price_type=xtconstant.FIX_PRICE,
                                                  price=bid_price_2)
            logger.info({"sell_stock order_id": order_id, "order_volume": avai_v, "stock_code": stock_code, "bid_price_2": bid_price_2})
            time.sleep(3)
            return {"sell_stock order_id": order_id, "order_volume": avai_v, "stock_code": stock_code}

    @classmethod
    def format_code(cls, code):
        code_type: str = get_stock_type(code)
        return code + "." + code_type.upper()

trader = AutoTrader()
trader.test_get_real_data()

def check_security_type(stock_code):
    """
    根据股票代码判断是ETF还是股票
    
    参数:
        stock_code (str): 股票代码，可以是带后缀的完整代码或不带后缀的纯数字代码
        
    返回:
        str: "ETF" - 表示是ETF基金
             "STOCK" - 表示是股票
             "UNKNOWN" - 表示无法识别类型
    """
    # 去除可能的前缀和后缀，只保留数字部分
    code = ''.join(filter(str.isdigit, stock_code))
    
    if not code:
        return "UNKNOWN"
    
    # 中国A股ETF代码规则（主要）
    etf_prefixes = {
        '15',  # 深交所ETF
        '51',  # 上交所ETF
        '58'   # 上交所科创板ETF
    }
    
    # 检查前两位是否在ETF前缀中
    if code[:2] in etf_prefixes:
        return "ETF"
    
    # 股票代码规则（主要）
    stock_prefixes = {
        '00',  # 深交所主板
        '30',  # 深交所创业板
        '60',  # 上交所主板
        '68'   # 上交所科创板
    }
    
    # 检查前两位是否在股票前缀中
    if code[:2] in stock_prefixes:
        return "STOCK"
    
    # 其他情况
    return "UNKNOWN"

def job():
    try:
        if is_stock_trading_time():
            logger.info("当前是交易时间")
            cache = cache_object["cache"]
            with get_connection() as cursor:
                sql = STOCK_SQL.format(today=datetime.today().strftime("%Y%m%d"))
                cursor.execute(sql)
                result = cursor.fetchall()
                now = datetime.now()
                now = now.strftime("%Y-%m-%d")

                if now not in cache:
                    cache[now] = {}
                    cache_object["cache"] = cache
                    cache_object["max_trade_times"] = 0

                if result:
                    logger.info(f"抓涨停列表为: {result}")
                    codes = [ele[0] for ele in result]
                    for code in codes:
                        if code not in cache[now] and cache_object["max_trade_times"]<10:
                            trader.buy_stock(trader.format_code(code), 10000)
                            cache[now][code] = 1
                            cache_object["cache"] = cache
                            cache_object["max_trade_times"] = cache_object["max_trade_times"]+1
                        else:
                            logger.info(f"{code} in cache object")
                        logger.info({"max_trade_times": cache_object["max_trade_times"]})
            now = datetime.now()
            positions = trader.xt_trader.query_stock_positions(trader.account)
            for position in positions:
                code = position.stock_code
                if get_increase_rate(code)>1 and check_security_type(code)=="STOCK":
                    trader.sell_stock(code)
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
    