# -*- coding: utf-8 -*-
import datetime
from codecs import open
import requests
from mysql_util import insert_table_by_batch
import emoji
from bs4 import BeautifulSoup


def scrape(language):
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:11.0) Gecko/20100101 Firefox/11.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8'
    }
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    url = 'https://github.com/trending/{language}'.format(language=language)
    r = requests.get(url, headers=HEADERS)
    assert r.status_code == 200

    soup = BeautifulSoup(r.text, 'html5lib')

    items = soup.find_all("article", class_="Box-row")
    ds = []
    for item in items:
        title = item.find('h2', class_='lh-condensed').find('a').text.strip().replace("\n", "")
        description_item = item.find("p")
        if description_item:
            description = emoji.demojize(item.find("p", class_='col-9').text.strip())
        else:
            description = ''
        url = item.find('a', class_='tooltipped').get('href').strip()
        url = "https://github.com" + url
        star_fork = list(item.find('div', class_='f6').find_all("a"))
        if len(star_fork) < 2:
            star = 0
            fork = 0
        else:
            star = star_fork[0].text.strip()
            fork = star_fork[1].text.strip()
            star = int(star.replace(',', ''))
            try:
                fork = int(fork.replace(',', ''))
            except:
                fork = 0
        try:
            new_star = item.find('span', class_='float-sm-right').text.strip()
            new_star = new_star.split(" ")[0]
            new_star = int(new_star.replace(',', ''))
        except:
            new_star = 0
        ds.append([today_str, language, title, url, description, star, fork, new_star])
    sql = '''
    replace into github.ods_github_trend
    values (%s, %s,%s,%s,%s, %s,%s,%s)
    '''
    insert_table_by_batch(sql, ds)


def job():
    # write markdown
    scrape('')  # full_url = 'https://github.com/trending?since=daily'
    scrape('python')
    scrape('java')
    scrape('javascript')
    scrape('go')
    scrape('scala')


if __name__ == '__main__':
    job()
