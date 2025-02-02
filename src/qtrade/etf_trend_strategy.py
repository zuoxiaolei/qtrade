import pandas as pd
import empyrical
import numpy as np
from mysql_util import *

CLOSE_NAME = "close"
weight = {'512690': 0.2534386069177697, '159632': 0.19455239012637038, '513660': 0.15975224413478367,
          '510880': 0.08285366080880825, '513500': 0.078776175845003, '512290': 0.07801606135486173,
          '159869': 0.06639370117394562, '515050': 0.04373830188635165, '516970': 0.04247885775210589}


class TradeSystem():
    def __init__(self):
        pass

    def get_data(self):
        sql = """select t1.*
            from etf.ods_etf_history t1
            join (
            select distinct a.code
            from etf.dim_etf_basic_info a
            join etf.dim_etf_scale b
              on a.code=b.code
            where b.scale>=50 and type like "%股票%"
            ) t2
            on t1.code=t2.code
            """
        with get_connection() as cursor:
            cursor.execute(sql)
            data = cursor.fetchall()
        df = pd.DataFrame(data, columns=['code', 'date', 'open', 'high', 'low', 'close', 'volume'])

        df = df.drop_duplicates(subset=["code", "date"])
        df = df.sort_values(by=["code", "date"])
        df["datetime"] = pd.to_datetime(df["date"])
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

    def buy_always(self, df, window):
        df["buy_always"] = 1
        _, profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df = self.buy_at_top(df, window,
                                                                                                     label_col="buy_always")
        return "buy_always", profit, accu_returns, annu_returns, max_drawdown, sharpe, all_profit_df

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
                  self.buy_at_top_bottom(df, window),
                  self.buy_always(df, window)
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
            "sell_at_top_buy_bottom": self.sell_at_top_buy_bottom,
            "buy_always": self.buy_always
        }
        return string_method_map[method_string](df, window)

    def main(self):
        df = self.get_data()
        codes = df.code.unique().tolist()
        evaluate_result = []
        start_time = time.time()
        for code in tqdm(codes):
            df_name = df.loc[df.code == code]
            if len(df_name) > 100:
                for windows in range(2, 30):
                    result = self.get_profit(df_name.copy(deep=True), windows)
                    # print(result)
                    for method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, _ in result:
                        evaluate_result.append(
                            [code, windows, method_string, profit, accu_returns, annu_returns, max_drawdown,
                             sharpe])
        end_time = time.time()
        print(f"cost {end_time - start_time} run get_profit")
        evaluate_result_df = pd.DataFrame(evaluate_result, columns=["code", "windows", 'method_string',
                                                                    'profit', 'accu_returns', 'annu_returns',
                                                                    'max_drawdown', 'sharpe'])
        evaluate_result_df = evaluate_result_df.sort_values(by="sharpe", ascending=False)
        evaluate_result_df.to_csv("evaluate_result2.csv", index=False)

    def get_portfolio(self):
        with get_connection() as cursor:
            sql = """select * from etf.ads_etf_best_param"""
            cursor.execute(sql)
            data = cursor.fetchall()
            df = pd.DataFrame(data,
                              columns=["code", "windows", 'method_string', 'profit', 'accu_returns', 'annu_returns',
                                       'max_drawdown', 'sharpe', 'weight'])
        codes = df["code"].unique().tolist()
        df_data = self.get_data()
        df_data = df_data[df_data.code.isin(codes)]
        all_dfs = []
        w = dict(df[["code", "weight"]].values.tolist())
        for index, row in tqdm(df.iterrows()):
            code, windows, method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, _ = row.to_list()
            code = str(code)
            df_name = df_data.loc[df_data.code == code]
            method_string, profit, accu_returns, annu_returns, max_drawdown, sharpe, df_profit = self.get_profit_by_method(
                df_name, windows, method_string)
            df_profit["code"] = code
            df_profit["datetime"] = df_profit["datetime"].map(lambda x: x.strftime("%Y-%m-%d"))
            all_dfs.append(df_profit)
            print(df_profit)

        df = pd.concat(all_dfs, axis=0)
        df = df.pivot(index="datetime", columns="code", values="increase_rate")
        df = df.fillna(0)
        df = df.replace([np.inf, -np.inf], np.nan)

        df.index = pd.to_datetime(df.index)
        df["portfolio"] = 0
        for k, v in w.items():
            df["portfolio"] = df["portfolio"] + v * df[k]
        print(df.tail())

        def calc_indicators(df_returns):
            accu_returns = empyrical.cum_returns_final(df_returns)
            annu_returns = empyrical.annual_return(df_returns)
            max_drawdown = empyrical.max_drawdown(df_returns)
            sharpe = empyrical.sharpe_ratio(df_returns)
            return accu_returns, annu_returns, max_drawdown, sharpe

        accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df["portfolio"])
        print(accu_returns, annu_returns, max_drawdown, sharpe)
        return df


def get_etf_matchless_report():
    trade_system = TradeSystem()
    df = trade_system.get_portfolio()
    df["date"] = df.index.map(lambda x: x.strftime("%Y-%m-%d"))
    data = df[["date", "portfolio"]].values.tolist()
    insert_table_by_batch("replace into etf.ads_matchless_portfolio_rpt values (%s, %s)", data)


if __name__ == '__main__':
    get_etf_matchless_report()
