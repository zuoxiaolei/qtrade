import requests
import pytz
from datetime import datetime
from mysql_util import get_connection

global_tokens = [
    'b0b21688d4694f7999c301386ee90a0c',  # xiaolei
    # 'a667be8c89e341348a854ee0707793c9',  # zhanglin
]
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y-%m-%d")


def send_message(content, title, tokens=global_tokens):
    for token in tokens:
        params = {
            'token': token,
            'title': title,
            'content': content,
            'template': 'txt'}
        url = 'http://www.pushplus.plus/send'
        res = requests.get(url, params=params)
        print(res, params)


def send_ratation_message(last_day, last_day_profit):
    print({"last_day": last_day, "now": now})
    last_day_profit = round(last_day_profit, 2)
    hour = datetime.now(tz).hour
    
    with get_connection() as cursor:
        cursor.execute("""select max(date) from etf.ods_etf_history""")
        max_date = cursor.fetchall()[0][0]
    max_month = max_date[:7]
    max_year = max_date[:4]
    
    with get_connection() as cursor:
        cursor.execute(f'''select value 
                            from etf.ads_etf_portfolio_profit_summary
                            where date='{max_month}' ''')
        increase_month = cursor.fetchall()[0][0]
        
    with get_connection() as cursor:
        cursor.execute(f'''select value 
                                              from etf.ads_etf_portfolio_profit_summary
                                               where date='{max_year}' ''')
        increase_year = cursor.fetchall()[0][0]

    increase_month = round(increase_month, 2)
    increase_year = round(increase_year, 2)
    if last_day == now and 9 <= hour <= 15:
        message = f"组合投资\n日期：{last_day}\n日涨幅：{last_day_profit}%\n月涨幅：{increase_month}%\n年涨幅：{increase_year}%"
        send_message(message, title="组合投资")

def send_nasdaq_strategy():
    with get_connection() as cursor:
        cursor.execute("""select code, date, buy_sell_label  from etf.ads_etf_rank_strategy_detail where code='159941' order by date desc limit 1""")
        data = cursor.fetchall()
    code, date, buy_sell_label = data[0]
    hour = datetime.now(tz).hour
    buy_label = "买" if buy_sell_label>=9 else "卖"
    if date==now and 9 <= hour <= 15:
        message = f"""
        日期：{date}
        代码：{code}
        得分：{buy_sell_label}
        买卖信号：{buy_label}"""
        send_message(message, title="纳斯达克策略", tokens=global_tokens+["66cc056f86954bccb2e2591c6b0227b6"])

if __name__ == '__main__':
    # send_ratation_message("2024-05-28", last_day_profit=1.0)
    send_nasdaq_strategy()
    