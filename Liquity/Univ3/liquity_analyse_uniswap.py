import os
from web3 import Web3
from dotenv import load_dotenv

import pandas as pd
import pickle as pkl

import dask.delayed as delayed
from dask import diagnostics

# init infura
load_dotenv()
infura_id = os.getenv("INFURA_API_ID")

infura_provider = Web3.HTTPProvider("https://mainnet.infura.io/v3/"+ infura_id)
w3 = Web3(infura_provider)

def get_timestamp(block_number):
    block = w3.eth.getBlock(block_number)
    timestamp = block['timestamp']
    return timestamp

# data cleaning
def data_cleaning(raw_df):
    
    # populate working df from flat logs
    args_df = pd.concat([raw_df['args'], raw_df['args'].apply(pd.Series)], axis=1)
    args_df = args_df.drop(columns=['args','sender', 'recipient','sqrtPriceX96','liquidity','tick'])
     
    # scale units & small housekeeping issues
    relevant_df = pd.DataFrame()
    relevant_df['block'] = raw_df['blockNumber']
    
    relevant_df["LUSD_amount"] = args_df["amount0"].div(10 ** 18)
    relevant_df["USDC_amount"] = args_df["amount1"].div(10 ** 6)
    
    # sort by block
    relevant_df.sort_values("block", ascending=True,  inplace=True)
    print('cleaned')
    return relevant_df

# load univ3 logs for lusd/usdc as Dask Delayed object, ie load in chunks instead of putting all in memory
@delayed
def load_data(file_path):
    with open(file_path, 'rb') as f:
        raw_data = pkl.load(f)
    raw_df = pd.DataFrame(raw_data)
    cleaned_df = data_cleaning(raw_df)
    return cleaned_df

if __name__ == "__main__":
    cleaned_df = load_data('/Users/jonas/Workspace/liquity_logs_uniswap_test.pkl')
    with diagnostics.ProgressBar():
        cleaned_df = cleaned_df.compute()
        cleaned_df["timestamp"] = cleaned_df["block"].apply(get_timestamp)
        cleaned_df = cleaned_df.reset_index().drop(columns=['index'])
    
    print('exporting data')
    print(cleaned_df)

    cleaned_df.to_feather("/Users/jonas/Workspace/Local/Drop/results/uniswap_logs_liquity.feather")
    
    print('success')