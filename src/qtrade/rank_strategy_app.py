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


def rotation_strategy():
    st.markdown("## 轮动策略")
    
    sql = '''
    select distinct date
    from etf.ads_etf_rank_strategy_detail
    order by date desc
    '''
    date = mysql_conn.query(sql, ttl=0)["date"].tolist()
    select_date = st.selectbox(label='年份', options=date)
    
    sql = f'''
    select t1.code, t3.name, t1.date, t1.buy_sell_label
    from etf.ads_etf_rank_strategy_detail t1
    join (select distinct code from etf.ads_etf_rank_stratgegy_params where sharpe>=1) t2
    on t1.code=t2.code
    join etf.dim_etf_basic_info t3
      on t1.code=t3.code
    where t1.date='{select_date}'
    order by date desc, buy_sell_label desc
    '''
    df = mysql_conn.query(sql, ttl=0)
    st.dataframe(df, hide_index=True, width=width, height=600)



def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe
