import streamlit as st

pg = st.navigation([st.Page("pages/qtrade_forex.py", title="外汇组合"),
                    st.Page("pages/qtrade_btc.py", title="btc策略"),
                    st.Page("pages/qtrade_etf.py", title="ETF组合"),
                    st.Page("pages/qtrade_matchless.py", title="USDCAD策略"),
                    st.Page("pages/qtrade_etf_matchless.py", title="ETF无双策略"),
                    st.Page("pages/qtrade_fund.py", title="基金组合")
                    ])
pg.run()
