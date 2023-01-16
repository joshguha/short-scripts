import pickle as pkl
import pandas as pd
import math

# load univ3 logs for lusd/usdc
with open("/Users/jonas/Workspace/logs.pkl", "rb") as f:
    object = pkl.load(f)
raw_df = pd.DataFrame(object)

# convert sqrtPriceX96 as per https://ethereum.stackexchange.com/a/112587
def compute_sqrt_ratio(price):
    LUSDdivUSDC = (int(price) * int(price) * int(1e18) >> (96 * 2)) / 1000000
    return LUSDdivUSDC

# data cleaning
def data_cleaning(raw_df):
    args_df = raw_df['args']
    args_df = pd.DataFrame()
    args_df[['sender', 'recipient', 'amount0', 'amount1', 'sqrtPriceX96','liquidity','tick']] = raw_df['args'].apply(lambda x: pd.Series(x))
    args_df['block'] = raw_df['blockNumber']
    
    # liquidity: The liquidity of the pool after the swap
    # sqrtPriceX96: The sqrt(price) of the pool after the swap, as a Q64.96
    # amount0: The delta of the balance of token0 of the pool, exact when negative, minimum when positive
    # amount1: The delta of the balance of token1 of the pool, exact when negative, minimum when positive
    relevant_df = args_df.loc[:, ['block','sqrtPriceX96', 'liquidity','amount0','amount1']]
    
    # apply sqrt computation & drop raw sqrt column
    relevant_df["price"] = relevant_df["sqrtPriceX96"].apply(compute_sqrt_ratio)
    relevant_df.drop(columns =['sqrtPriceX96'], inplace=True)
    relevant_df.rename(columns = {'amount0':'LUSD','amount1':'USDC'}, inplace = True)
    
    # scale units
    relevant_df["LUSD"] = relevant_df["LUSD"] / (10 ** 18)
    relevant_df["USDC"] = relevant_df["USDC"] / (10 ** 6)
    # currently wrong?!
    relevant_df["liquidity"] = relevant_df["liquidity"] / (10 ** 18)

    return relevant_df

# output
cleaned_df = data_cleaning(raw_df)

print(cleaned_df)
cleaned_df.to_csv("/Users/jonas/Workspace/Local/Drop/liquity_univ3.csv")

# specific block
# data_structure = cleaned_df.loc[cleaned_df['block']=='16389986']
# print(data_structure)