import pandas as pd
import streamlit as st
import pymysql
from streamlit_echarts import st_echarts
import empyrical
from datetime import datetime
import pytz
from dateutil.relativedelta import relativedelta
import math
import empyrical

pymysql.install_as_MySQLdb()
tz = pytz.timezone('Asia/Shanghai')
now = datetime.now(tz)

# 计算上个月的日期
last_month = now - relativedelta(months=1)
last_month = last_month.strftime("%Y%m")
print(last_month)

is_local = False
ttl = 600
height = 740
width = 800
mysql_conn = st.connection('mysql', type='sql', ttl=ttl)


# def rotation_strategy():
#     st.markdown("## 纳斯达克策略score是否大于等于7")
    
#     sql = f'''
#     select *
#     from etf.ads_etf_rank_strategy_detail t 
#     where code='159941'
#     order by date desc
#     '''
#     df = mysql_conn.query(sql, ttl=0)
#     st.dataframe(df, hide_index=True, width=width, height=600)


def back_test(is_buy_array, close_array, date_array):
    profit = 1
    last_status = 0
    profits = []
    
    for i in range(1, len(close_array)):
        is_buy = is_buy_array[i]
        last_price = close_array[i-1]
        current_price = close_array[i]
        profit = profit*(current_price/last_price) if last_status else profit
        profits.append([date_array[i], profit, is_buy])
        last_status = is_buy
        
    df_profits = pd.DataFrame(profits, columns=["date", "rate", "status"])
    df_profits.index = df_profits["date"]
    df_profits["increase_rate"] = df_profits["rate"] / df_profits["rate"].shift() - 1
    sharpe = empyrical.sharpe_ratio(df_profits["increase_rate"])
    max_drawdown = empyrical.max_drawdown(df_profits["increase_rate"])
    annual_return = math.pow(profit, 365/len(df_profits)) - 1
    return profit, sharpe, annual_return, max_drawdown, df_profits

def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe

def rotation_strategy():
    st.markdown("## nasdaq策略")

    # 筛选时间
    sql = f'''
    select t1.code, t1.date, t1.buy_sell_label, t2.close
    from etf.ads_etf_rank_strategy_detail t1
    join etf.ods_etf_history t2
      on t1.code=t2.code and t1.date=t2.date
    where t1.code='159941'
    order by t1.date
    '''
    df_portfolio = mysql_conn.query(sql, ttl=0)
    st.dataframe(df_portfolio.tail(100)[::-1], hide_index=True, width=width, height=600)
    min_date = df_portfolio.date.min()
    max_date = df_portfolio.date.max()
    is_buy_array = df_portfolio.apply(lambda x: x["buy_sell_label"]>=7, axis=1) - 0
    is_buy_array = is_buy_array.values
    close_array =  df_portfolio["close"].values
    date_array = df_portfolio["date"].values
    profit, sharpe, annual_return, max_drawdown, df_profits = back_test(is_buy_array, close_array, date_array)
    df_profits = df_profits[["date", "rate"]]
    df_profits = df_profits.reset_index(drop=True)
    df_portfolio = df_portfolio.merge(df_profits, on="date")

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
