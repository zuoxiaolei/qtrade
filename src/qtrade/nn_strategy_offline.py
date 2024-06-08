import optuna
import pandas as pd
import numpy as np
import warnings
import empyrical
from scipy.special import erf
from functools import partial
from tqdm import tqdm
from mysql_util import get_connection

warnings.filterwarnings(action="ignore")

def softmax(x):
    """Softmax激活函数，用于输出层"""
    e_x = np.exp(x - np.max(x, axis=-1, keepdims=True))
    return e_x / np.sum(e_x, axis=-1, keepdims=True)

def gelu(x):
    """Implementation of the GELU activation function."""
    return 0.5 * x * (1 + erf(x / np.sqrt(2)))

def leaky_relu(x, alpha=0.01):
    return np.maximum(alpha * x, x)


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
    sharpe = empyrical.sharpe_ratio(df_profits["increase_rate"])
    if np.isnan(sharpe) or np.isinf(sharpe) and profit<1.1:
        sharpe = 0
    return profit, sharpe, df_profits


# Define an objective function to be minimized.
def objective(trial: optuna.Trial, df=None):
    n_row = 20
    n_col = 3
    low = -1
    high = 1
    parameters_layer1 = np.zeros(shape=(n_row, n_col))
    for i in range(n_row):
        for j in range(n_col):
            parameters_layer1[i][j] = trial.suggest_float(f"{i}_{j}", low, high)
    b = trial.suggest_float("b", low, high)
            
    parameters_layer2 = np.zeros(shape=(n_col, n_col))
    for i in range(n_col):
        for j in range(n_col):
            parameters_layer2[i][j] = trial.suggest_float(f"2_{i}_{j}", low, high)
    b2 = trial.suggest_float("b2", low, high)
            
    def get_action(ts):
        input_array = np.array(ts.tolist()).reshape(1, -1)
        output = gelu(np.dot(input_array, parameters_layer1)+b)
        output2 = softmax(np.dot(output, parameters_layer2)+b2)
        action = np.argmax(output2, axis=-1)
        action = action[0]
        assert action in (0, 1, 2)
        return action
    profit, sharpe, df_profits = get_profit(df, get_action, n_row)
    return profit, sharpe

def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe

def eval(df, p_dict):
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
    
    profit, sharpe, df_profits = get_profit(df, get_action, n_row)
    accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df_profits["increase_rate"])
    return accu_returns, annu_returns, max_drawdown, sharpe

def get_all_parameter():
    df_all = pd.read_parquet("etf.parquet")
    codes = df_all.code.unique().tolist()
    for code in tqdm(codes):
        df = df_all[df_all.code==code]
        df = df.drop_duplicates(subset=["date"])
        df = df.dropna(axis=0)
        df = df.sort_values(by="date")
        df["increase_rate"] = df["close"].pct_change()*100
        if len(df)>1000:
            study = optuna.create_study(directions=["maximize", "maximize"])  # Create a new study.
            my_objective = partial(objective, df=df)
            study.optimize(my_objective, n_trials=10000, show_progress_bar=True)
            
            best_parameter = None
            for i, best_trial in enumerate(study.best_trials):
                print(f"The {i}-th Pareto solution was found at Trial#{best_trial.number}.")
                print(f"  Params: {best_trial.params}")
                f1, f2 = best_trial.values
                print(f"  Values: {f1=}, {f2=}")
                best_parameter = best_trial.params
            accu_returns, annu_returns, max_drawdown, sharpe = eval(df, best_parameter)
            with get_connection() as cursor:
                sql = '''
                replace into etf.ads_nn_strategy_parameter values (%s, %s, %s, %s)
                '''
                cursor.execute(sql, (code, annu_returns, sharpe, str(best_parameter)))


#   Params: {'0_0': 0.49267922776351014, '0_1': -0.8744218908843551, '0_2': -0.6401598341518042, '1_0': -0.9457411894160577, '1_1': -0.034791883573543014, '1_2': -0.12983458494335887, '2_0': -0.23540269039123563, '2_1': -0.8485640497950659, '2_2': -0.5022896872556923, '3_0': -0.9933990522499927, '3_1': 0.39904123723371154, '3_2': 0.8185255939795499, '4_0': 0.028750217782829157, '4_1': 0.5640671011367389, '4_2': 0.26014305859700815, '5_0': 0.719885723123094, '5_1': -0.889010947233811, '5_2': 0.8812602331203567, '6_0': 0.12004037085913444, '6_1': -0.7821538135789206, '6_2': 0.05367422526207433, '7_0': -0.793074494791528, '7_1': 0.2729785257610322, '7_2': 0.025735023577919014, '8_0': -0.8159904727084186, '8_1': -0.2568842466142627, '8_2': -0.09955176386293152, '9_0': 0.15379028663379501, '9_1': 0.4947725546326909, '9_2': 0.6588293695972958, '10_0': 0.24942978373240066, '10_1': -0.8164894328270798, '10_2': 0.1559598788059755, '11_0': 0.087693239002959, '11_1': 0.9852919932978612, '11_2': 0.3664684968655163, '12_0': -0.9885889930530363, '12_1': 0.1105851643946143, '12_2': 0.16565399356207222, '13_0': -0.002513340878833592, '13_1': -0.2529389244522118, '13_2': -0.5101559052787015, '14_0': -0.4976818687859632, '14_1': 0.024633169476793837, '14_2': -0.6475296320354689, '15_0': -0.7942293290893756, '15_1': -0.5306671349068335, '15_2': 0.09093764500484247, '16_0': 0.0897152231550975, '16_1': 0.7936344384183129, '16_2': -0.6487331157293881, '17_0': -0.46280756163885517, '17_1': 0.30216528428588907, '17_2': 0.04888722315712424, '18_0': 0.6064999285152644, '18_1': 0.9979206479473972, '18_2': -0.2134214778284973, '19_0': -0.5233346210367749, '19_1': 0.5312892825200004, '19_2': 0.3826552439070734, 'b': -0.7630564411099341, '2_0_0': 0.7772766423370132, '2_0_1': -0.5983307734955283, '2_0_2': 0.34615643188069756, '2_1_0': 0.5257521250495947, '2_1_1': -0.5049441278112086, '2_1_2': 0.3681001868873406, '2_2_0': -0.03567863906867075, '2_2_1': -0.7831995088957517, '2_2_2': 0.45739491228913365, 'b2': 0.3964233500447851}
#   Values: f1=6.627403013134795, f2=1.3019192463895195

if __name__ == "__main__":
    get_all_parameter()
    