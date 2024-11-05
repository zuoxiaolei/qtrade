import streamlit as st


pg = st.navigation([st.Page("pages/qtrade_forex.py", title="外汇组合"), 
                    st.Page("pages/qtrade_etf.py", title="ETF组合"),])
pg.run()
