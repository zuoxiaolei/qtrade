import pandas as pd
import streamlit as st

import pymysql
from streamlit_echarts import st_echarts
import empyrical

pymysql.install_as_MySQLdb()

is_local = False
ttl = 600
height = 740
width = 800
st.set_page_config(layout='wide')
index_name = 'rsrs指标'
columns = ['股票代码', '股票名称', '股票规模',
           '日期', '价格', 'rsrs指标',
           '昨天rsrs指标', 'rsrs买入阈值',
           'rsrs卖出阈值', '买卖信号']
mysql_conn = st.connection('mysql', type='sql', ttl=ttl)
max_date_sql = '''
                select max(date) date 
                from etf.ods_etf_history
               '''
max_date = mysql_conn.query(max_date_sql, ttl=ttl).values.tolist()[0][0]


def portfolio_strategy():
    st.markdown("## 组合投资策略")
    # 筛选时间
    sql = '''
    select date, rate 
    from etf.ads_eft_portfolio_rpt
    order by date
    '''
    df_portfolio = mysql_conn.query(sql, ttl=0)

    sql_stock = '''
    select code, t1.date, close rate
    from etf.ods_etf_history t1
    join (select distinct date from etf.ads_eft_portfolio_rpt) t2
      on t1.date=t2.date
    where code in ('518880', '512890', '159941') 
              and t1.date>= '2019-01-21'
    order by code, date
    '''

    df_stock_dict = {'all': df_portfolio}

    min_date = df_portfolio.date.min()
    max_date = df_portfolio.date.max()
    options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
    options = [str(ele) for ele in options]
    options = ['all'] + options
    st.markdown("### 收益曲线")
    select_year = st.selectbox(label='年份', options=options)
    if select_year != 'all':
        for key in df_stock_dict.keys():
            df = df_stock_dict[key]
            df_stock_dict[key] = df[df.date.map(lambda x: x[:4] == select_year)]

    for key, value in df_stock_dict.items():
        value.index = pd.to_datetime(value['date'])
        value['rate'] = value['rate'] / value['rate'].iloc[0]
        value["profit"] = value["rate"] / value["rate"].shift() - 1

    df_portfolio.index = pd.to_datetime(df_portfolio['date'])
    df_portfolio['rate'] = df_portfolio['rate'] / df_portfolio['rate'].iloc[0]
    df_portfolio["profit"] = df_portfolio["rate"] / df_portfolio["rate"].shift() - 1

    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_stock_dict["all"]['profit'])
    accu_returns = round(accu_returns, 3)
    annu_returns = round(annu_returns, 3)
    max_drawdown = round(max_drawdown, 3)
    sharpe = round(sharpe, 3)

    min_values = [df_stock_dict[ele]["rate"].min() for ele in df_stock_dict]
    max_values = [df_stock_dict[ele]["rate"].max() for ele in df_stock_dict]

    min_value = round(min(min_values) * 0.98, 2)
    max_value = round(max(max_values) * 1.02, 2)

    options = {
        "xAxis": {
            "type": "category",
            "data": df_stock_dict['all']['date'].tolist(),
        },
        "yAxis": {"type": "value", 'min': min_value, 'max': max_value},
        "series": [
            {"data": value['rate'].tolist(), "type": "line"} for key, value in df_stock_dict.items()],
        "tooltip": {
            'trigger': 'axis',
            'backgroundColor': 'rgba(32, 33, 36,.7)',
            'borderColor': 'rgba(32, 33, 36,0.20)',
            'borderWidth': 1,
            'textStyle': {
                'color': '#fff',
                'fontSize': '12'
            },
            'axisPointer': {
                'type': 'cross',
                'label': {
                    'backgroundColor': '#6a7985'
                }
            },
        },
        "title": {
            'text': f'''累计收益: {accu_returns}\n年化收益: {annu_returns}\n最大回撤:{max_drawdown}\n夏普比:{sharpe}''',
            'right': 'left',
            'top': '0px',
        }
    }
    st_echarts(options=options, height=400)

    df_portfolio = df_portfolio.reset_index(drop=True)
    df_portfolio["profit_str"] = df_portfolio["profit"].map(lambda x: str(round(100 * x, 3)) + "%")

    df_portfolio_daily = df_portfolio[['date', 'profit_str']]
    df_portfolio_daily = df_portfolio_daily.sort_values("date", ascending=False)
    df_portfolio_daily.columns = ['日期', '收益率']
    df_portfolio_daily = df_portfolio_daily.head(100)

    df_portfolio_month = df_portfolio
    df_portfolio_month["月份"] = df_portfolio["date"].map(lambda x: str(x[:7]))
    df_portfolio_month = df_portfolio_month.groupby("月份", as_index=False)["profit"].sum()
    df_portfolio_month["profit_str"] = df_portfolio_month["profit"].map(lambda x: str(round(100 * x, 3)) + "%")
    df_portfolio_month = df_portfolio_month[['月份', "profit_str"]]
    df_portfolio_month.columns = ['月份', '收益率']
    df_portfolio_month = df_portfolio_month.sort_values(by="月份", ascending=False)
    st.markdown("### 每月收益率分析")
    st.dataframe(df_portfolio_month, hide_index=True, width=width, height=300)
    st.markdown("### 每日收益率分析")
    st.dataframe(df_portfolio_daily, hide_index=True, width=width, height=300)


def get_hot_invest():
    sql = '''
    select date, query, hotScore stock_score
    from github.baidu_hot_news
    where date >= (select date from etf.dim_etf_trade_date where rn=7)
    order by date desc, stock_score desc
    '''
    df_news = mysql_conn.query(sql, ttl=0)
    df_news.columns = ['日期', '新闻标题', '对股市影响大小']
    st.markdown("# 每日热点新闻分析")
    st.dataframe(df_news, hide_index=True, width=width, height=1000)


def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe


def get_okex_list():
    sql = '''
    select t1.date, t1.unique_name, t1.nick_name,
			 CONCAT('https://www.okx.com/cn/copy-trading/account/', t1.unique_name, '?tab=swap') profile_url,
			 follow_pnl,
			 win_rate,
			 yield_rate,
			 lever
from okex.user_info_daily t1
join okex.user_level t2
  on t1.unique_name=t2.unique_name
where date=(SELECT max(date) from okex.user_info_daily) and lever<=10 and follow_pnl>0
order by t1.follow_pnl desc
    '''
    df_news = mysql_conn.query(sql, ttl=0)
    df_news.columns = ['日期', '用户标识', '昵称', '个人主页', '收益', '胜率', '回报率', '杠杆倍数']
    st.markdown("# okex列表")
    df_news['用户标识'] = df_news.apply(lambda x: f"[{x['用户标识']}]({x['个人主页']})", axis=1)
    df_news['昵称'] = df_news.apply(lambda x: f"[{x['昵称']}]({x['个人主页']})", axis=1)
    df_news = df_news.drop("个人主页", axis=1)
    st.markdown(df_news.to_markdown(index=False), unsafe_allow_html=True)


portfolio, hot_new, okex_strategy = st.tabs(["组合投资", "投资热点", "okex列表推荐"])

with portfolio:
    portfolio_strategy()

with hot_new:
    get_hot_invest()

with okex_strategy:
    get_okex_list()
