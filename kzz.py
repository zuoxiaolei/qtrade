import easyquotation
import json
import re
import time

import requests
quotation = easyquotation.use('sina')
userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36"
header = {
    "Referer": "https://www.jisilu.cn/data/cbnew/",
    'User-Agent': userAgent,
    'Cookie': "kbzw__Session=5u6136maucteofr8hv36qpm9k7; kbz_newcookie=1; kbzw_r_uname=macrochen; kbzw__user_login=7Obd08_P1ebax9aX5MPZ0-ba293PkZyh6dbc7OPm1Nq_1KKq2sSkktnepaGrpKeZrpSsraqxlaeT2q_Wnd-l2MWsmJiyoO3K1L_RpKagrKWuk5ecpMy01r6bruPz3tXlzaaXpJGXn8DZxNnP6Ojo0bSMwNDqxuOXwNnEkLDHmc2JqpzWk6rAqKCTudHgzdnQ2svE1euRq5SupaaugZisvM3CtaWM48vhxpe-2NvM34qUvN3b6Nncka-RpaehrJWjkaKyqInMzd3D6MqmrKavj6OX; Hm_lvt_164fe01b1433a19b507595a43bf58262=1619880428,1620457500; Hm_lpvt_164fe01b1433a19b507595a43bf58262=1620978364"
}


def check_stock_board(code):
    """
    判断股票代码是否属于科创板或创业板。
    """
    if code.startswith('688'):
        return False
    elif code.startswith('300'):
        return False
    else:
        return True

def get_content():
    # https://32.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124045700749086112435_1634389030530&pn=3&pz=100&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f243&fs=b:MK0354&fields=f2,f3,f12,f14,f229,f230,f237&_=1634389030541
    url = "http://32.push2.eastmoney.com/api/qt/clist/get?cb=jQuery1124045700749086112435_" + str(int(round(time.time() * 1000))) + "&pn=1&pz=400&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f243&fs=b:MK0354&fields=f2,f3,f12,f14,f229,f230,f237,f232&_=" + str(int(round(time.time() * 1000)))

    response = requests.get(url)
    code = response.status_code
    if code != 200:
        print("获取数据失败， 状态码：" + code)

    content = response.text

    return parse_content(content)


def parse_content(content):
    data = None
    try:
        data = json.loads(re.match(".*?({.*}).*", content, re.S).group(1))
    except:
        raise ValueError('Invalid Input')

    # 所有数据行
    rows = data['data']['diff']

    if len(rows) == 0:
        print("未获取到数据。")
    return rows


content = get_content()
s_list = list(set([ele['f232'] for ele in content]))
result = {}
realtime_dict = quotation.stocks(s_list)
for ele in realtime_dict:
    close = realtime_dict[ele]["close"]
    now = realtime_dict[ele]["now"]
    name = realtime_dict[ele]["name"]
    rate = (now/close-1)*100
    
    if check_stock_board(ele) and rate>=9.9:
        print(ele, name, rate)
    if not check_stock_board(ele) and rate>=19.9:
        print(ele, name, rate)

