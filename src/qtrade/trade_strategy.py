import pandas as pd
import time
import empyrical
from tqdm import tqdm
import riskfolio as rp
import numpy as np
from mysql_util import get_connection, insert_table_by_batch
from datetime import datetime
from loguru import logger
from concurrent.futures import ProcessPoolExecutor, as_completed

pd.set_option('display.float_format', '{:.10f}'.format)

CLOSE_NAME = "close"


def get_df_label(df, windows):
    CLOSE_NAME = "close"
    df["max_value"] = df[CLOSE_NAME].rolling(window=windows).max()
    df["min_value"] = df[CLOSE_NAME].rolling(window=windows).min()
    df["buy_at_top"] = df.apply(lambda x: x[CLOSE_NAME] >= x["max_value"], axis=1) - 0
    df["buy_at_buttom"] = df.apply(lambda x: x[CLOSE_NAME] <= x["min_value"], axis=1) - 0
    df["sell_at_top"] = df["buy_at_top"] * -1
    df["sell_at_bottom"] = df["buy_at_buttom"] * -1
    df["buy_at_top_bottom"] = df.apply(
        lambda x: x[CLOSE_NAME] >= x["max_value"] or x[CLOSE_NAME] <= x["min_value"], axis=1) - 0
    df["sell_at_top_botton"] = df["buy_at_top_bottom"] * -1

    def get_buy_at_top_sell_botton_label(row):
        close = row.close
        max_value = row.max_value
        min_value = row.min_value
        if close >= max_value:
            return 1
        elif close <= min_value:
            return -1
        else:
            return 0

    df["buy_at_top_sell_botton"] = df.apply(get_buy_at_top_sell_botton_label, axis=1)
    df["sell_at_top_buy_bottom"] = -df["buy_at_top_sell_botton"]
    return df


class TradeSystem():
    def __init__(self):
        self.portfolio_weight = {'XAUUSD': 1.0}

    def get_data(self):
        df = pd.read_parquet(r"D:\workspace\strategy\df_full_1h.parquet")
        df = df.drop_duplicates(subset=["time", "name"])
        df = df.sort_values(by=["name", "time"])
        df["datetime"] = pd.to_datetime(df["time"], unit='s', utc=True)
        df['datetime'] = df['datetime'].dt.tz_convert('Asia/Shanghai')
        df["hour"] = df["datetime"].dt.hour
        return df

    def get_data_from_mysql(self):
        sql = """
        select t1.*, t2.windows, sharpe, method_string
        from mt5.ads_forex_history_data_1h t1
        join mt5.ads_forex_best_param t2
          on t1.code=t2.name and t1.hour=t2.hour
        order by t1.date
        """
        with get_connection() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
        df = pd.DataFrame(data, columns=["date", "code", "close", "hour", "windows", "shape", 'method_string'])
        df["datetime"] = df["date"].map(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        return df

    def calc_indicators(self, df_returns):
        accu_returns = empyrical.cum_returns_final(df_returns)
        annu_returns = empyrical.annual_return(df_returns)
        max_drawdown = empyrical.max_drawdown(df_returns)
        sharpe = empyrical.sharpe_ratio(df_returns)
        return accu_returns, annu_returns, max_drawdown, sharpe

    def trade_at_method_string(self, df, window, method_string):
        max_len = len(df)
        close_array = df[CLOSE_NAME].tolist()
        is_buy_array = df[method_string].tolist()
        close_datetime = df["datetime"].tolist()
        all_profit = []
        all_datetime = []
        profit = 1
        last_status = 0

        for i in range(window, max_len):
            last_status = is_buy_array[i - 1]
            last_price = close_array[i - 1]
            current_price = close_array[i]

            if last_status == 1:
                profit = profit * (current_price / last_price)
            elif last_status == -1:
                profit = profit * (last_price / current_price)
            else:
                profit = profit
            all_profit.append(profit)
            all_datetime.append(close_datetime[i])
        all_profit_df = pd.DataFrame(list(zip(all_datetime, all_profit)), columns=["datetime", "increase_rate"])
        all_profit_df.index = all_profit_df.datetime
        all_profit_df["increase_rate"] = all_profit_df["increase_rate"] / all_profit_df["increase_rate"].shift() - 1
        accu_returns, annu_returns, max_drawdown, sharpe = self.calc_indicators(all_profit_df["increase_rate"])
        return method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def get_profit_by_method(self, df, window, method_string):
        df = get_df_label(df, window)
        return self.trade_at_method_string(df, window, method_string)

    def main(self):
        df = self.get_data()
        names = df.name.unique().tolist()
        evaluate_result = []
        start_time = time.time()
        method_strings = ["buy_at_top", "buy_at_buttom", "sell_at_top",
                          "sell_at_bottom", "buy_at_top_bottom", "sell_at_top_botton",
                          "buy_at_top_sell_botton", "sell_at_top_buy_bottom"]
        logger.info({"names": len(names)})
        for name in tqdm(names):
            futures = []
            df_name = df.loc[df.name == name]
            hours = df_name.hour.unique().tolist()
            for hour in hours:
                df_hour = df_name.loc[df_name.hour == hour]
                if len(df_hour) > 100:
                    for windows in range(2, 30):
                        df_label = get_df_label(df_hour, windows)
                        for method_string in method_strings:
                            method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, _ = self.trade_at_method_string(
                                df_label.copy(deep=True), windows, method_string)
                            logger.info((method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe))
                            evaluate_result.append(
                                [name, hour, windows, method_string, profit, accu_returns, annu_returns, max_drawdown,
                                 sharpe])
        end_time = time.time()
        print(f"cost {end_time - start_time} run get_profit")
        evaluate_result_df = pd.DataFrame(evaluate_result, columns=["code", "hour", "windows", 'method_string',
                                                                    'profit', 'accu_returns', 'annu_returns',
                                                                    'max_drawdown', 'sharpe'])
        evaluate_result_df = evaluate_result_df.sort_values(by="sharpe", ascending=False)
        evaluate_result_df.to_csv("evaluate_result.csv", index=False)

    def get_best_portfolio(self):
        df = pd.read_csv(r"D:\workspace\qtrade\src\qtrade\evaluate_result2.csv")
        # names = ["CADCHF", "CADCHF", "CADCHF", "EURCHF", "EURCAD", "USDJPY", "GBPCAD", "DXY", "USDCAD", "USDCHF", "GBPNZD", "US30", "EURAUD", "AUDCAD"]
        # df = df[df.name.isin(names)]
        # not_name = ["USOIL", "AUDUSD", "BTCUSD", "XAUUSD", "XAUUSD", "AUDNZD", "AUDJPY", "EURJPY", "GBPAUD", "US500",
        #             "GBPCHF", "GBPJPY", "GBPJPY", "GBPCAD"]
        # names = [ele for ele in df.name.unique().tolist() if ele not in not_name]

        names = ("GBPUSD", "DXY", "EURGBP", "USDCAD", "AUDCHF", "EURCHF", "EURCAD", "NZDCAD", "AUDNZD",
                 "USDJPY", "GBPNZD", "AUDCAD", "GBPCAD", "GBPJPY", "CADCHF", "AUDUSD", "BTCUSD")
        df = df[df.code.isin(names)]
        df_data = self.get_data()
        all_dfs = []
        for index, row in tqdm(df.iterrows()):
            name, hour, windows, method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe = row.to_list()
            df_name = df_data.loc[df_data.name == name]
            df_hour = df_name.loc[df_name.hour == hour]
            df_hour = get_df_label(df_hour, windows)
            method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, df_profit = self.trade_at_method_string(
                df_hour, windows, method_string)
            df_profit["name"] = name
            df_profit["datetime"] = df_profit["datetime"].map(lambda x: x.strftime("%Y-%m-%d"))
            all_dfs.append(df_profit)
        df = pd.concat(all_dfs, axis=0)
        df = df.pivot(index="datetime", columns="name", values="increase_rate")
        df.to_csv("temp.csv")
        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.dropna(axis=0, how="any")
        print(df.isna().sum())
        port = rp.Portfolio(returns=df)
        method_mu = 'hist'  # Method to estimate expected returns based on historical data.
        method_cov = 'hist'
        port.assets_stats(method_mu=method_mu, method_cov=method_cov)
        model = 'Classic'
        rm = 'MV'
        obj = 'Sharpe'
        hist = True
        rf = 0
        l = 0
        w = port.optimization(model=model, rm=rm, obj=obj, rf=rf, l=l, hist=hist)
        w = w.sort_values(by="weights", ascending=False)
        w["name"] = w.index
        print(w)
        print(dict(w[["name", "weights"]].values.tolist()))
        w.to_csv("portfolio_w.csv", index=False)

        df.index = pd.to_datetime(df.index)
        df["portfolio"] = 0
        for k, v in w.to_dict()["weights"].items():
            df["portfolio"] = df["portfolio"] + v * df[k] * 100

        def calc_indicators(df_returns):
            accu_returns = empyrical.cum_returns_final(df_returns)
            annu_returns = empyrical.annual_return(df_returns)
            max_drawdown = empyrical.max_drawdown(df_returns)
            sharpe = empyrical.sharpe_ratio(df_returns)
            return accu_returns, annu_returns, max_drawdown, sharpe

        accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df["portfolio"])
        print(accu_returns, annu_returns, max_drawdown, sharpe)
        (df["portfolio"] + 1).cumprod().plot(figsize=(15, 5))
        import matplotlib.pylab as plt
        plt.show()

    def get_portfolio(self):
        df_data = self.get_data_from_mysql()
        df_data = df_data.sort_values(by=["code", "date"], ascending=True)
        df = df_data[["code", "hour", "windows", "method_string"]].drop_duplicates()

        all_dfs = []
        for index, row in tqdm(df.iterrows()):
            name, hour, windows, method_string = row.to_list()
            df_hour = df_data.loc[(df_data.code == name) & (df_data.hour == hour)]
            method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, df_profit = self.get_profit_by_method(
                df_hour, windows, method_string)
            df_profit["name"] = name
            df_profit["datetime"] = df_profit["datetime"].map(lambda x: x.strftime("%Y-%m-%d"))
            all_dfs.append(df_profit)
        df = pd.concat(all_dfs, axis=0)

        df = df.pivot(index="datetime", columns="name", values="increase_rate")
        df = df.fillna(0)
        df = df.replace([np.inf, -np.inf], np.nan)

        df.index = pd.to_datetime(df.index)
        df["portfolio"] = 0
        for k, v in self.portfolio_weight.items():
            df["portfolio"] = df["portfolio"] + v * df[k]

        def calc_indicators(df_returns):
            accu_returns = empyrical.cum_returns_final(df_returns)
            annu_returns = empyrical.annual_return(df_returns)
            max_drawdown = empyrical.max_drawdown(df_returns)
            sharpe = empyrical.sharpe_ratio(df_returns)
            return accu_returns, annu_returns, max_drawdown, sharpe

        accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df["portfolio"])
        df["date"] = df.index.map(lambda x: x.strftime("%Y-%m-%d"))
        logger.info(
            {"accu_returns, annu_returns, max_drawdown, sharpe": (accu_returns, annu_returns, max_drawdown, sharpe)})
        df["portfolio"] = df["portfolio"]
        return df, self.portfolio_weight


if __name__ == '__main__':
    import time

    start_time = time.time()
    trade_system = TradeSystem()
    df, _ = trade_system.get_portfolio()
    end_time = time.time()
    print(end_time - start_time)

    df: pd.DataFrame = df.sort_values(by="date", ascending=True)
    # df = df.tail(100)
    print({"df": df})
    data = df[['date', 'portfolio']].values.tolist()
    sql = """replace into mt5.ads_forex_portfolio_rpt
             values (%s, %s)
    """
    insert_table_by_batch(sql, data)
    
    # 权重
    # df = pd.read_csv(r"D:\workspace\qtrade\src\qtrade\evaluate_result2.csv")
    # df = df[df.code.isin([ele for ele in trade_system.portfolio_weight])]
    # data = df.values.tolist()
    # sql = """replace into mt5.ads_forex_best_param
    #          values (%s, %s,%s, %s,%s, %s,%s, %s,%s)
    # """
    # insert_table_by_batch(sql, data)
