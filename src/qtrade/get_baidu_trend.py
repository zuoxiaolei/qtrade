from mysql_util import insert_table_by_batch, get_connection
import datetime
import requests
from bertClassify.news_classify_onnx import news2_label

today_str = datetime.datetime.now().strftime('%Y-%m-%d')


def get_baidu_hot_news():
    url = 'https://top.baidu.com/api/board?platform=wise&tab=realtime'
    header = {
        'User-Agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Mobile Safari/537.36',
        'Host': 'top.baidu.com',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': 'https://top.baidu.com/board?tab=novel',
    }
    r = requests.get(url, header)
    json_data = r.json()
    content = json_data['data']['cards'][0]['content']
    fields = [(today_str, ele['appUrl'], ele['desc'], ele['query'], ele['hotScore'],) for ele in content]
    sql = '''replace into github.baidu_hot_news(`date`, app_url, `desc`, `query`, hotScore) values(%s, %s, %s, %s, %s)'''
    insert_table_by_batch(sql, fields)


def update_news_analysis():
    sql = '''
    select date, app_url, query
    from github.baidu_hot_news
    where category is null
    '''

    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()

    news_string_array = [ele[-1] for ele in data]
    news_label = news2_label(news_string_array)
    result = []
    for (date, app_url, query), label in zip(data, news_label):
        result.append([label, date, app_url])
    if result:
        sql = '''update github.baidu_hot_news set category=%s
                                 where date=%s and app_url=%s'''
        insert_table_by_batch(sql, result)


if __name__ == '__main__':
    get_baidu_hot_news()
    update_news_analysis()
