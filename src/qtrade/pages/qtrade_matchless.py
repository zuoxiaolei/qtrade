import pandas as pd
import streamlit as st
import pymysql
from streamlit_echarts import st_echarts
import empyrical
import os
from trade_strategy import TradeSystem
from mysql_util import *
pymysql.install_as_MySQLdb()


ttl = 600
height = 740
width = 800
trade_system = TradeSystem()


# mysql_conn = st.connection('mysql', type='sql', ttl=ttl)
# max_date_sql = '''
#                 select substring(max(date), 1, 10) date
#                 from mt5.ads_forex_history_data
#                '''
# max_date = mysql_conn.query(max_date_sql, ttl=ttl).values.tolist()[0][0]
#
# data_sql = """
# select substring(date, 1, 10) date, code, close
# from mt5.ads_forex_history_data
# order by date
# """


def forex_portfolio_strategy():
    st.cache_data.clear()
    st.cache_resource.clear()
    st.markdown("## 无双投资策略")

    with get_connection() as cursor:
        sql = """
        select date, increase_rate 
        from mt5.ads_forex_portfolio_rpt
        order by date
        """
        cursor.execute(sql)
        data = cursor.fetchall()
    
    Y = pd.DataFrame(data, columns=["date", "portfolio"])
    Y["portfolio"] = Y["portfolio"] * 100


    min_date = Y.date.min()
    max_date = Y.date.max()
    df_portfolio = Y
    options = list(range(int(min_date[:4]), int(max_date[:4]) + 1))[::-1]
    options = [str(ele) for ele in options]
    options = options + ["all"]

    st.markdown("### 收益曲线")
    select_year = st.selectbox(label='年份', options=options, key=34)

    if select_year != 'all':
        df_portfolio = Y[Y.date.map(lambda x: str(x)[:4] == select_year)]

    df_portfolio.index = pd.to_datetime(df_portfolio['date'])
    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_portfolio['portfolio'])

    df_portfolio["portfolio_cum"] = (df_portfolio["portfolio"] + 1).cumprod()
    df_portfolio["mean10"] = df_portfolio["portfolio_cum"].rolling(10, min_periods=1).mean()
    df_portfolio_show = df_portfolio.tail(50)
    df_portfolio_show = df_portfolio_show[["portfolio_cum", "mean10"]]
    df_portfolio_show = df_portfolio_show.sort_values(by="date", ascending=False)
    df_portfolio_show["label"] = (df_portfolio_show["portfolio_cum"] > df_portfolio_show["mean10"]) - 0
    st.dataframe(df_portfolio_show.tail(50))

    min_values = [df_portfolio["portfolio_cum"].min()]
    max_values = [df_portfolio["portfolio_cum"].max()]
    min_value = round(min(min_values) * 0.9999, 4)
    max_value = round(max(max_values) * 1.0001, 4)

    options = {
        "xAxis": {
            "type": "category",
            "data": df_portfolio['date'].tolist(),
        },
        "yAxis": {"type": "value", 'min': min_value, 'max': max_value},
        "series": [
            {"data": df_portfolio['portfolio_cum'].tolist(), "type": "line"},
            {"data": df_portfolio['mean10'].tolist(), "type": "line"},
        ],
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

    df_portfolio["profit"] = df_portfolio["portfolio"]
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

    df_portfolio_year = df_portfolio
    df_portfolio_year["年份"] = df_portfolio_year["date"].map(lambda x: str(x[:4]))
    df_portfolio_year = df_portfolio_year.groupby("年份", as_index=False)["profit"].sum()
    df_portfolio_year["收益率"] = df_portfolio_year["profit"].map(lambda x: str(round(100 * x, 3)) + "%")
    df_portfolio_year = df_portfolio_year.sort_values(by="年份", ascending=False)

    day, month, year = st.tabs(['每日收益率分析', '每月收益率分析', '每年收益率分析'])

    with day:
        st.markdown("### 每日收益率分析")
        st.bar_chart(df_portfolio_daily.head(12), x='date', y='profit')
        st.dataframe(df_portfolio_daily[['date', 'profit_str']], hide_index=True, width=width, height=300)

    with month:
        st.markdown("### 每月收益率分析")
        st.bar_chart(df_portfolio_month.head(12), x='月份', y='profit')
        st.dataframe(df_portfolio_month[['月份', '收益率']], hide_index=True, width=width, height=300)

    with year:
        st.markdown("### 每年收益率分析")
        st.bar_chart(df_portfolio_year.head(12), x='年份', y='profit')
        st.dataframe(df_portfolio_year[['年份', '收益率']], hide_index=True, width=width, height=300)


def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    accu_returns = round(accu_returns, 3)
    annu_returns = round(annu_returns, 3)
    max_drawdown = round(max_drawdown, 3)
    sharpe = round(sharpe, 3)
    return accu_returns, annu_returns, max_drawdown, sharpe


forex_portfolio_strategy()
