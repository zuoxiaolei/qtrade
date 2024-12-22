import pandas as pd
import time
import empyrical
from tqdm import tqdm
import riskfolio as rp
import numpy as np
from mysql_util import get_connection, insert_table_by_batch
from datetime import datetime

CLOSE_NAME = "close"


class TradeSystem():
    def __init__(self):
        self.portfolio_weight = {'EURGBP': 0.10250194107993126, 'GBPUSD': 0.10099189184810654,
                                 'EURCAD': 0.09853762448124613,
                                 'EURCHF': 0.08088330598926935, 'CADCHF': 0.07052817241288686,
                                 'USDCHF': 0.0625871750572771,
                                 'USDJPY': 0.061286347628406644, 'DXY': 0.05595054329233643,
                                 'USDCAD': 0.0418416133787629,
                                 'EURUSD': 0.04133431844509363, 'GBPNZD': 0.04086672398630775,
                                 'EURAUD': 0.039089627864273536,
                                 'US30': 0.0379627830594683, 'NZDCAD': 0.03679808030797965,
                                 'AUDCAD': 0.035339463911515503,
                                 'USTEC': 0.034704953462031396, 'AUDCHF': 0.03053545971500302,
                                 'CHFJPY': 0.028259974080104017}

    def get_data(self):
        df = pd.read_parquet("df_full_1h.parquet")
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

    def buy_at_top(self, df, window, label_col="is_top"):
        max_len = len(df)
        close_array = df[CLOSE_NAME].tolist()
        is_buy_array = df[label_col].tolist()
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
        return "buy_at_top", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def buy_at_buttom(self, df, window, label_col="is_bottom"):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col=label_col)
        return "buy_at_buttom", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def sell_at_top(self, df, window):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="sell_at_top_label")
        return "sell_at_top", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def sell_at_bottom(self, df, window, label_col="sell_at_bottom_label"):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col=label_col)
        return "sell_at_bottom", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def buy_at_top_bottom(self, df, window):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="is_top_or_bottom")
        return "buy_at_top_bottom", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def sell_at_top_botton(self, df, window):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="sell_at_top_botton_label")
        return "sell_at_top_botton", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def buy_at_top_sell_botton(self, df, window):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="buy_at_top_sell_botton_label")
        return "buy_at_top_sell_botton", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def sell_at_top_buy_bottom(self, df, window):
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="sell_at_top_buy_bottom_label")
        return "sell_at_top_buy_bottom", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

    def get_buy_at_top_sell_botton_label(self, row):
        close = row.close
        max_value = row.max_value
        min_value = row.min_value
        if close >= max_value:
            return 1
        elif close <= min_value:
            return -1
        else:
            return 0

    def get_sell_at_top_buy_bottom_label(self, row):
        return -self.get_buy_at_top_sell_botton_label(row)

    def get_sell_at_top_label(self, row):
        close = row.close
        max_value = row.max_value
        if close >= max_value:
            return -1
        else:
            return 0

    def get_sell_at_bottom_label(self, row):
        close = row.close
        min_value = row.min_value
        if close <= min_value:
            return -1
        else:
            return 0

    def get_profit(self, df, window=10):
        '''
        method "buy_at_top"  "sell_at_top" "buy_at_bottom"  "sell_at_bottom"
        '''
        df["max_value"] = df[CLOSE_NAME].rolling(window=window).max()
        df["min_value"] = df[CLOSE_NAME].rolling(window=window).min()
        df["is_top"] = df.apply(lambda x: x[CLOSE_NAME] >= x["max_value"], axis=1) - 0
        df["is_bottom"] = df.apply(lambda x: x[CLOSE_NAME] <= x["min_value"], axis=1) - 0
        df["is_top_or_bottom"] = df.apply(lambda x: x["max_value"] >= x[CLOSE_NAME] or x["min_value"] <= x[CLOSE_NAME],
                                          axis=1) - 0
        df["buy_at_top_sell_botton_label"] = df.apply(lambda x: self.get_buy_at_top_sell_botton_label(x), axis=1)
        df["sell_at_top_buy_bottom_label"] = df.apply(lambda x: self.get_sell_at_top_buy_bottom_label(x), axis=1)
        df["sell_at_top_label"] = df.apply(lambda x: self.get_sell_at_top_label(x), axis=1)
        df["sell_at_bottom_label"] = df.apply(lambda x: self.get_sell_at_bottom_label(x), axis=1)
        df["sell_at_top_botton_label"] = -df["is_top_or_bottom"]

        result = [self.buy_at_top(df, window),
                  self.buy_at_buttom(df, window),
                  self.sell_at_top(df, window),
                  self.sell_at_bottom(df, window),
                  self.buy_at_top_bottom(df, window),
                  self.sell_at_top_botton(df, window),
                  self.buy_at_top_sell_botton(df, window),
                  self.sell_at_top_buy_bottom(df, window),
                  ]
        return result

    def get_profit_by_method(self, df, window, method_string):
        df["max_value"] = df[CLOSE_NAME].rolling(window=window).max()
        df["min_value"] = df[CLOSE_NAME].rolling(window=window).min()
        df["is_top"] = df.apply(lambda x: x["max_value"] == x[CLOSE_NAME], axis=1) - 0
        df["is_bottom"] = df.apply(lambda x: x["min_value"] == x[CLOSE_NAME], axis=1) - 0
        df["is_top_or_bottom"] = df.apply(lambda x: x["max_value"] == x[CLOSE_NAME] or x["min_value"] == x[CLOSE_NAME],
                                          axis=1) - 0
        df["buy_at_top_sell_botton_label"] = df.apply(lambda x: self.get_buy_at_top_sell_botton_label(x), axis=1)
        df["sell_at_top_buy_bottom_label"] = df.apply(lambda x: self.get_sell_at_top_buy_bottom_label(x), axis=1)
        df["sell_at_top_label"] = df.apply(lambda x: self.get_sell_at_top_label(x), axis=1)
        df["sell_at_bottom_label"] = df.apply(lambda x: self.get_sell_at_bottom_label(x), axis=1)
        df["sell_at_top_botton_label"] = -df["is_top_or_bottom"]

        string_method_map = {
            "buy_at_top": self.buy_at_top,
            "buy_at_buttom": self.buy_at_buttom,
            "sell_at_top": self.sell_at_top,
            "sell_at_bottom": self.sell_at_bottom,
            "buy_at_top_bottom": self.buy_at_top_bottom,
            "sell_at_top_botton": self.sell_at_top_botton,
            "buy_at_top_sell_botton": self.buy_at_top_sell_botton,
            "sell_at_top_buy_bottom": self.sell_at_top_buy_bottom
        }
        return string_method_map[method_string](df, window)

    def main(self):
        df = self.get_data()
        names = df.name.unique().tolist()
        evaluate_result = []
        names = ["USDCAD"]
        start_time = time.time()
        for name in names:
            df_name = df.loc[df.name == name]
            hours = df_name.hour.unique().tolist()
            # hours = [0]
            for hour in hours:
                df_hour = df_name.loc[df_name.hour == hour]
                if len(df_hour) > 100:
                    for windows in range(2, 30):
                        result = self.get_profit(df_hour.copy(deep=True), windows)
                        print(result)
                        for method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe in result:
                            evaluate_result.append(
                                [name, hour, windows, method_string, profit, accu_returns, annu_returns, max_drawdown,
                                 sharpe])
        end_time = time.time()
        print(f"cost {end_time - start_time} run get_profit")
        evaluate_result_df = pd.DataFrame(evaluate_result, columns=["name", "hour", "windows", 'method_string',
                                                                    'profit', 'accu_returns', 'annu_returns',
                                                                    'max_drawdown', 'sharpe'])
        evaluate_result_df = evaluate_result_df.sort_values(by="sharpe", ascending=False)
        evaluate_result_df.to_csv("evaluate_result2.csv", index=False)

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
        df = df.dropna(axis=0)
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
        return df, self.portfolio_weight
        # print(accu_returns, annu_returns, max_drawdown, sharpe)
        # (df["portfolio"] + 1).cumprod().plot(figsize=(15, 5))
        # import matplotlib.pylab as plt
        # plt.show()


if __name__ == '__main__':
    import time
    start_time = time.time()
    trade_system = TradeSystem()
    trade_system.get_portfolio()
    end_time = time.time()
    print(end_time-start_time)
