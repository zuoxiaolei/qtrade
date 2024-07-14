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
    st.markdown("## 纳斯达克策略score是否大于等于7")
    
    sql = f'''
    select *
    from etf.ads_etf_rank_strategy_detail t 
    where code='159941'
    order by date desc
    '''
    df = mysql_conn.query(sql, ttl=0)
    st.dataframe(df, hide_index=True, width=width, height=600)
