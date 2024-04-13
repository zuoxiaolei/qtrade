import requests
import pytz
from datetime import datetime

tokens = [
    'b0b21688d4694f7999c301386ee90a0c',  # xiaolei
    # 'a667be8c89e341348a854ee0707793c9',  # zhanglin
]
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz).strftime("%Y-%m-%d")


def send_message(content):
    for token in tokens:
        params = {
            'token': token,
            'title': '组合投资策略',
            'content': content,
            'template': 'txt'}
        url = 'http://www.pushplus.plus/send'
        res = requests.get(url, params=params)
        print(res, params)


def send_ratation_message(last_day, last_day_profit):
    print({"last_day": last_day, "now": now})
    last_day_profit = round(last_day_profit, 2)
    hour = datetime.now(tz).hour
    if last_day == now and 9 <= hour <= 15:
        message = f"组合投资\n日期：{last_day}\n涨幅：{last_day_profit}%"
        send_message(message)


if __name__ == '__main__':
    pass
