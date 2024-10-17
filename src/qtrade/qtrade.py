import datetime
import pandas as pd
import streamlit as st
import pymysql
from streamlit_echarts import st_echarts
import empyrical
from easyquotation import use
from qtrade_forex import forex_portfolio_strategy
import numpy as np

pymysql.install_as_MySQLdb()

quotation = use('sina')
portfolio_code = ['518880', '512890', '159941']
portfolio_name = ['黄金ETF', '红利ETF', '纳指ETF']

is_local = False
ttl = 600
height = 740
width = 800

mysql_conn = st.connection('mysql', type='sql', ttl=ttl)
max_date_sql = '''
                select max(date) date 
                from etf.ods_etf_history
               '''
max_date = mysql_conn.query(max_date_sql, ttl=ttl).values.tolist()[0][0]


def get_realtime_increase_rate(stock_code):
    realtime_dict = quotation.stocks([stock_code])
    now = realtime_dict[0][stock_code]['now']
    close = realtime_dict[0][stock_code]['close']
    increase_rate = now / close - 1
    return increase_rate


def get_portfolio_realtime_data(stock_code):
    realtime_dict = quotation.stocks(stock_code[1:])
    all_increase_rate = [0] * len(stock_code)
    date_str = None
    for num, code in enumerate(stock_code):
        if num > 0:
            now = realtime_dict[code]['now']
            close = realtime_dict[code]['close']
            date_str = realtime_dict[code]['date']
            increase_rate = round((now / close - 1) * 100, 2)
            all_increase_rate[num] = increase_rate

    all_increase_rate[0] = (0.5 * all_increase_rate[1] +
                            0.3 * all_increase_rate[2] +
                            0.2 * all_increase_rate[3]
                            )
    all_increase_rate[0] = round(all_increase_rate[0], 2)
    return all_increase_rate, date_str


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
    all_increase_rate, max_date = get_portfolio_realtime_data(['all'] + portfolio_code)
    max_year = max_date[:4]
    sql_max_month = """
    select max(date) date
    from etf.ads_etf_portfolio_profit_summary
    where date_type='month'
    """
    max_month = mysql_conn.query(sql_max_month, ttl=0)['date'].iloc[0]

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
    options = options + ['all']

    st.markdown("### 收益曲线")
    select_year = st.selectbox(label='年份', options=options, key=3)
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


def get_params(df):
    df = df.sort_values(by="date")
    df = df.drop_duplicates(subset=["date"])
    df["c"] = df.close.map(float)
    df["future_increase_rate1"] = (df.c.shift(-1) / df.c - 1) * 100

    df["date"] = df["date"].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"))
    df["year"] = df["date"].map(lambda x: x.year)
    mean_rate = df["future_increase_rate1"].mean()

    def get_rank(series):
        data = series.values
        return np.argsort(-data)[-1]

    for i in range(2, 21):
        df[f"c_rank{i}"] = df.c.rolling(i).apply(get_rank)

    # get model parameter
    feature_score = {}
    for i in range(2, 21):
        tmp = df.groupby(f"c_rank{i}", as_index=False)["future_increase_rate1"].mean()
        tmp = tmp[tmp["future_increase_rate1"] >= mean_rate]
        feature_score[f"c_rank{i}"] = tmp[f"c_rank{i}"].tolist()
    model = feature_score
    keys = list(model.keys())

    def get_rank_strategy(row):
        score = 0
        for ele in keys:
            if row[ele] in model[ele]:
                score += 1
        return score

    df["buy_sell_label"] = df.apply(get_rank_strategy, axis=1)
    return df, model


def back_test(is_buy_array, close_array, date_array):
    profit = 1
    last_status = 0
    profits = []

    for i in range(1, len(close_array)):
        is_buy = is_buy_array[i]
        last_price = close_array[i - 1]
        current_price = close_array[i]
        profit = profit * (current_price / last_price) if last_status else profit
        profits.append([date_array[i], profit, is_buy])
        last_status = is_buy

    df_profits = pd.DataFrame(profits, columns=["date", "rate", "status"])
    df_profits.index = df_profits["date"]
    df_profits["increase_rate"] = df_profits["rate"] / df_profits["rate"].shift() - 1
    sharpe = empyrical.sharpe_ratio(df_profits["increase_rate"])
    max_drawdown = empyrical.max_drawdown(df_profits["increase_rate"])
    annual_return = empyrical.annual_return(df_profits["increase_rate"])
    return profit, sharpe, annual_return, max_drawdown, df_profits


forex_portfolio, etf_portfolio = st.tabs(['外汇组合', 'ETF组合'])

with forex_portfolio:
    forex_portfolio_strategy()

with etf_portfolio:
    portfolio_strategy()
