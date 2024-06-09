import pandas as pd
import streamlit as st

import pymysql
from streamlit_echarts import st_echarts
import empyrical
from easyquotation import use
from nn_strategy_app import nn_strategy

quotation = use('sina')
portfolio_code = ['518880', '512890', '159941']
portfolio_name = ['黄金ETF', '红利ETF', '纳指ETF']
pymysql.install_as_MySQLdb()

is_local = False
ttl = 600
height = 740
width = 800
# st.set_page_config(layout='wide')

mysql_conn = st.connection('mysql', type='sql', ttl=ttl)
max_date_sql = '''
                select max(date) date 
                from etf.ods_etf_history
               '''
max_date = mysql_conn.query(max_date_sql, ttl=ttl).values.tolist()[0][0]


def get_portfolio_realtime_data(stock_code):
    realtime_dict = quotation.stocks(stock_code[1:])
    all_increase_rate = [0] * len(stock_code)
    for num, code in enumerate(stock_code):
        if num > 0:
            now = realtime_dict[code]['now']
            close = realtime_dict[code]['close']
            increase_rate = round((now / close - 1) * 100, 2)
            all_increase_rate[num] = increase_rate

    all_increase_rate[0] = (0.5 * all_increase_rate[1] +
                            0.3 * all_increase_rate[2] +
                            0.2 * all_increase_rate[3]
                            )
    all_increase_rate[0] = round(all_increase_rate[0], 2)
    return all_increase_rate


def portfolio_strategy():
    st.markdown("## 组合投资策略")

    # 筛选时间
    sql = '''
    select date, rate 
    from etf.ads_eft_portfolio_rpt
    order by date
    '''
    df_portfolio = mysql_conn.query(sql, ttl=0)

    min_date = df_portfolio.date.min()
    max_date = df_portfolio.date.max()
    max_month = max_date[:7]
    max_year = max_date[:4]

    all_increase_rate = get_portfolio_realtime_data(['all'] + portfolio_code)
    increase_rate = all_increase_rate[1:]
    increase_month = mysql_conn.query(f'''select value 
                                          from etf.ads_etf_portfolio_profit_summary
                                           where date='{max_month}' ''', ttl=0)['value'].iloc[0]

    increase_year = mysql_conn.query(f'''select value 
                                              from etf.ads_etf_portfolio_profit_summary
                                               where date='{max_year}' ''', ttl=0)['value'].iloc[0]
    increase_month = round(increase_month, 2)
    increase_year = round(increase_year, 2)
    increase_rate = [str(ele) + "%" for ele in increase_rate]
    df_increase = pd.DataFrame(list(zip(portfolio_code, portfolio_name, increase_rate)),
                               columns=['股票代码', '股票名称', '股票涨幅'])
    day, month, year = st.columns(3)
    with day:
        st.metric(label=f'{max_date}组合日涨幅', value='',
                  delta=str(all_increase_rate[0]) + "%",
                  delta_color="inverse"
                  )
    with month:
        st.metric(label=f'{max_month}组合月涨幅', value='',
                  delta=str(increase_month) + "%",
                  delta_color="inverse"
                  )
    with year:
        st.metric(label=f'{max_year}组合年涨幅', value='',
                  delta=str(increase_year) + "%",
                  delta_color="inverse"
                  )
    st.markdown("")
    st.dataframe(df_increase, hide_index=True, width=width, height=140)
    options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
    options = [str(ele) for ele in options]
    options = ['all'] + options

    st.markdown("### 收益曲线")
    select_year = st.selectbox(label='年份', options=options)
    if select_year != 'all':
        df_portfolio = df_portfolio[df_portfolio.date.map(lambda x: x[:4] == select_year)]

    df_portfolio.index = pd.to_datetime(df_portfolio['date'])
    df_portfolio['rate'] = df_portfolio['rate'] / df_portfolio['rate'].iloc[0]
    df_portfolio["profit"] = df_portfolio["rate"] / df_portfolio["rate"].shift() - 1

    df_portfolio.index = pd.to_datetime(df_portfolio['date'])
    df_portfolio['rate'] = df_portfolio['rate'] / df_portfolio['rate'].iloc[0]
    df_portfolio["profit"] = df_portfolio["rate"] / df_portfolio["rate"].shift() - 1

    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_portfolio['profit'])
    accu_returns = round(accu_returns, 3)
    annu_returns = round(annu_returns, 3)
    max_drawdown = round(max_drawdown, 3)
    sharpe = round(sharpe, 3)

    min_values = [df_portfolio["rate"].min()]
    max_values = [df_portfolio["rate"].max()]
    min_value = round(min(min_values) * 0.98, 2)
    max_value = round(max(max_values) * 1.02, 2)

    options = {
        "xAxis": {
            "type": "category",
            "data": df_portfolio['date'].tolist(),
        },
        "yAxis": {"type": "value", 'min': min_value, 'max': max_value},
        "series": [
            {"data": df_portfolio['rate'].tolist(), "type": "line"}],
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
    df_portfolio_daily = df_portfolio[['date', 'profit_str', "profit"]]
    df_portfolio_daily = df_portfolio_daily.sort_values("date", ascending=False)
    df_portfolio_daily = df_portfolio_daily.head(100)
    df_portfolio_month = df_portfolio
    df_portfolio_month["月份"] = df_portfolio["date"].map(lambda x: str(x[:7]))
    df_portfolio_month = df_portfolio_month.groupby("月份", as_index=False)["profit"].sum()
    df_portfolio_month["收益率"] = df_portfolio_month["profit"].map(lambda x: str(round(100 * x, 3)) + "%")
    df_portfolio_month = df_portfolio_month.sort_values(by="月份", ascending=False)

    month, year, day = st.tabs(['每月收益率分析', '每年收益率分析', '每日收益率分析'])
    with month:
        st.markdown("### 每月收益率分析")
        st.bar_chart(df_portfolio_month.head(12), x='月份', y='profit')
        st.dataframe(df_portfolio_month[['月份', '收益率']], hide_index=True, width=width, height=300)

    with year:
        df_year = mysql_conn.query(
            "select date, value from etf.ads_etf_portfolio_profit_summary where date_type='year' ")
        df_year = df_year.sort_values(by='date', ascending=False)
        df_year["收益率"] = df_year.value.map(lambda x: str(round(x, 2)) + "%")
        st.markdown("### 每月收益率分析")
        st.bar_chart(df_year.head(12), x='date', y='value')
        st.dataframe(df_year[['date', '收益率']], hide_index=True, width=width, height=300)

    with day:
        st.markdown("### 每日收益率分析")
        st.bar_chart(df_portfolio_daily.head(12), x='date', y='profit')
        st.dataframe(df_portfolio_daily[['date', 'profit_str']], hide_index=True, width=width, height=300)


def get_hot_invest():
    sql = '''
    select date, query, hotScore stock_score
    from github.baidu_hot_news
    where date >= (select date from etf.dim_etf_trade_date where rn=7) and category in ('finance', 'stocks', 'politics')
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


def macro_strategy():
    sql = '''
    select date, close, 
		   avg(close) over (order by date rows 4 preceding) avg5,
			 avg(close) over (order by date rows 19 preceding) avg20
    from stock.micro_index t
    order by date desc
    limit 100
    '''
    df_macro = mysql_conn.query(sql, ttl=0)
    df_macro['is_bull'] = df_macro.apply(lambda x: int(x['avg5'] > x['avg20']), axis=1)
    df_macro = df_macro[['date', 'is_bull']]
    st.dataframe(df_macro, hide_index=True, width=width, height=1000)


# def board_strategy():
#     st.markdown("## 题材股策略")
#
#     # 筛选时间
#     sql = '''
#     select date, board_name, profit rate
#     from stock.board_profit
#     order by date
#     '''
#     df_portfolio = mysql_conn.query(sql, ttl=0)
#
#     min_date = df_portfolio.date.min()
#     max_date = df_portfolio.date.max()
#     options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
#     options = [str(ele) for ele in options]
#     options = ['all'] + options
#
#     st.markdown("### 收益曲线")
#     select_year = st.selectbox(label='年份', options=options, key="board")
#     if select_year != 'all':
#         df_portfolio = df_portfolio[df_portfolio.date.map(lambda x: x[:4] == select_year)]
#
#     df_portfolio.index = pd.to_datetime(df_portfolio['date'])
#     df_portfolio["profit"] = df_portfolio["rate"] / df_portfolio["rate"].shift() - 1
#
#     df_portfolio.index = pd.to_datetime(df_portfolio['date'])
#     df_portfolio['rate'] = df_portfolio['rate'] / df_portfolio['rate'].iloc[0]
#     df_portfolio["profit"] = df_portfolio["rate"] / df_portfolio["rate"].shift() - 1
#
#     accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_portfolio['profit'])
#     accu_returns = round(accu_returns, 3)
#     annu_returns = round(annu_returns, 3)
#     max_drawdown = round(max_drawdown, 3)
#     sharpe = round(sharpe, 3)
#
#     min_value = round(df_portfolio["rate"].max() * 0.98, 2)
#     max_value = round(df_portfolio["rate"].max() * 1.02, 2)
#
#     options = {
#         "xAxis": {
#             "type": "category",
#             "data": df_portfolio['date'].tolist(),
#         },
#         "yAxis": {},
#         "series": [{"data": df_portfolio['rate'].tolist(), "type": "line"}],
#         "tooltip": {
#             'trigger': 'axis',
#             'backgroundColor': 'rgba(32, 33, 36,.7)',
#             'borderColor': 'rgba(32, 33, 36,0.20)',
#             'borderWidth': 1,
#             'textStyle': {
#                 'color': '#fff',
#                 'fontSize': '12'
#             },
#             'axisPointer': {
#                 'type': 'cross',
#                 'label': {
#                     'backgroundColor': '#6a7985'
#                 }
#             },
#         },
#         "title": {
#             'text': f'''累计收益: {accu_returns}\n年化收益: {annu_returns}\n最大回撤:{max_drawdown}\n夏普比:{sharpe}''',
#             'right': 'left',
#             'top': '0px',
#         }
#     }
#     st_echarts(options=options, height=400)
#
#     df_portfolio = df_portfolio.reset_index(drop=True)
#     df_portfolio["profit_str"] = df_portfolio["profit"].map(lambda x: str(round(100 * x, 3)) + "%")
#
#     df_portfolio_daily = df_portfolio[['date', 'profit_str', "profit"]]
#     df_portfolio_daily = df_portfolio_daily.sort_values("date", ascending=False)
#     df_portfolio_daily = df_portfolio_daily.head(100)
#
#     df_portfolio_month = df_portfolio
#     df_portfolio_month["月份"] = df_portfolio["date"].map(lambda x: str(x[:7]))
#     df_portfolio_month = df_portfolio_month.groupby("月份", as_index=False)["profit"].sum()
#     df_portfolio_month["收益率"] = df_portfolio_month["profit"].map(lambda x: str(round(100 * x, 3)) + "%")
#     df_portfolio_month = df_portfolio_month.sort_values(by="月份", ascending=False)
#     st.markdown("### 每月收益率分析")
#     st.bar_chart(df_portfolio_month.head(12), x='月份', y='profit')
#     st.dataframe(df_portfolio_month[['月份', '收益率']], hide_index=True, width=width, height=300)
#     st.markdown("### 每日收益率分析")
#     st.bar_chart(df_portfolio_daily.head(12), x='date', y='profit')
#     st.dataframe(df_portfolio_daily[['date', 'profit_str']], hide_index=True, width=width, height=300)
#
#     st.markdown("### 板块轮动")
#     df_board = df_portfolio.tail(30)
#     df_board = df_board[['date', 'board_name', 'rate']]
#     df_board.sort_values(by="date", ascending=False, inplace=True)
#     st.dataframe(df_board, hide_index=True, width=width, height=300)


nn, portfolio, hot_new = st.tabs(["神经网络策略", "组合投资", "投资热点"])

with nn:
    nn_strategy()

with portfolio:
    portfolio_strategy()

with hot_new:
    get_hot_invest()




# with board_strategy_ui:
#     board_strategy()
