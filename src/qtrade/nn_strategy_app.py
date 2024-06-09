import pandas as pd
import streamlit as st
import pymysql
from streamlit_echarts import st_echarts
import empyrical
import numpy as np
from scipy.special import erf
from datetime import timedelta, datetime
import pytz
from dateutil.relativedelta import relativedelta


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

def softmax(x):
    """Softmax激活函数，用于输出层"""
    e_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
    return e_x / np.sum(e_x, axis=-1, keepdims=True)

def gelu(x):
    """Implementation of the GELU activation function."""
    return 0.5 * x * (1 + erf(x / np.sqrt(2)))

def get_profit(df, get_action, window):
    df["is_buy"] = df.increase_rate.rolling(window).apply(get_action)    
    max_len = len(df)
    close_array = df["close"].tolist()
    is_buy_array = df["is_buy"].tolist()
    dates = df["date"].tolist()
    profit = 1
    last_status = 0
    profits = []
    
    for i in range(window, max_len):
        all_is_buy = is_buy_array[i]
        last_price = close_array[i-1]
        current_price = close_array[i]
        profit = profit*(current_price/last_price) if last_status else profit
        if all_is_buy==1:
            last_status = 1
        elif all_is_buy==0:
            last_status = 0
        else:
            last_status = last_status
        profits.append([dates[i], profit, last_status])
    df_profits = pd.DataFrame(profits)
    df_profits.index = df_profits[0]
    df_profits["increase_rate"] = df_profits[1] / df_profits[1].shift() - 1
    df_profits.columns = ['date', 'rate', 'status', 'increase_rate']
    return df_profits

def get_profit_df(df, p_dict):
    n_row = 20
    n_col = 3
    parameters_layer1 = np.zeros(shape=(n_row, n_col))
    for i in range(n_row):
        for j in range(n_col):
            parameters_layer1[i][j] = p_dict[f"{i}_{j}"]
    b = p_dict["b"]
            
    parameters_layer2 = np.zeros(shape=(n_col, n_col))
    for i in range(n_col):
        for j in range(n_col):
            parameters_layer2[i][j] = p_dict[f"2_{i}_{j}"]
    b2 = p_dict["b2"]
            
    def get_action(ts):
        input_array = np.array(ts.tolist()).reshape(1, -1)
        output = gelu(np.dot(input_array, parameters_layer1)+b)
        output2 = softmax(np.dot(output, parameters_layer2)+b2)
        action = np.argmax(output2, axis=-1)
        action = action[0]
        assert action in (0, 1, 2)
        return action
    df_profits = get_profit(df, get_action, n_row)
    return df_profits

def nn_strategy():
    st.markdown("## 神经网络投资")
    sql = f'''
    select t1.code, t2.name, t1.parameter
    from etf.ads_nasdaq_strategy t1
    join etf.dim_etf_basic_info t2
      on t1.code=t2.code
    where t1.date='{last_month}'
    '''
    df_codes = mysql_conn.query(sql, ttl=0)
    name2code = dict(df_codes[['name', 'code']].values.tolist())
    code2parameter = dict(df_codes[['code', 'parameter']].values.tolist())
    
    # get history data
    options = list(name2code.keys())
    select_fund = st.selectbox(label='ETF名称', options=options)
    select_code = name2code[select_fund]
    sql = f'''
    select code, date, close 
    from etf.ods_etf_history
    where code='{select_code}'
    order by date
    '''.format(select_code=select_code)
    df = mysql_conn.query(sql, ttl=0)
    
    # get profit df
    df = df.dropna(axis=0)
    df = df.sort_values(by="date")
    df["increase_rate"] = df["close"].pct_change()*100
    p_dict = eval(code2parameter[select_code])
    df_portfolio = get_profit_df(df, p_dict)
    df_buy_sell = df.copy(deep=True).sort_values(by="date", ascending=False).head(100)
    df_buy_sell.columns = ["ETF代码", "日期", "收盘价", "涨幅%", "信号"]
    st.markdown("### 策略结果")
    st.dataframe(df_buy_sell, hide_index=True, width=width, height=300)
    
    min_date = df_portfolio.date.min()
    max_date = df_portfolio.date.max()
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



def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe
