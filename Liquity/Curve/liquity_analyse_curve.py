import pandas as pd
import numpy as np

# import data
data = pd.read_feather('/Users/jonas/Workspace/Local/Drop/results/raw_liquity_logs_curve.feather')

def normalize_names_curve_raw(curve_df_raw):
    curve_df_raw['price'] = None
    
    tokenSold_replacement_dict = {'0x5f98805a4e8be255a32880fdec7f6728c6568ba0': 'LUSD', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC','0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT','0x6b175474e89094c44da98b954eedeac495271d0f':'DAI','0x6c3f90f043a72fa612cbac8115ee7e52bde6e490':'3pool'}
    tokenBought_replacement_dict = {'0x5f98805a4e8be255a32880fdec7f6728c6568ba0': 'LUSD', '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC','0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT','0x6b175474e89094c44da98b954eedeac495271d0f':'DAI','0x6c3f90f043a72fa612cbac8115ee7e52bde6e490':'3pool'}

    curve_df_raw['tokenSold'] = curve_df_raw['tokenSold'].map(tokenSold_replacement_dict)
    curve_df_raw['tokenBought'] = curve_df_raw['tokenBought'].map(tokenBought_replacement_dict)
    curve_df_raw[['amountSold','amountBought']] = curve_df_raw[['amountSold','amountBought']].astype(float)

    return curve_df_raw

def normalize_curve(curve_df):
    
    curve_df = curve_df.drop(columns=['id'])
    curve_df = curve_df.sort_values(by='block', ascending=True)
    
    curve_df['USDC_amount'] = curve_df['amountSold'].where(curve_df['tokenSold'] == 'USDC').mul(-1)
    curve_df['USDT_amount'] = curve_df['amountSold'].where(curve_df['tokenSold'] == 'USDT').mul(-1)
    curve_df['LUSD_amount'] = curve_df['amountSold'].where(curve_df['tokenSold'] == 'LUSD').mul(-1)
    curve_df['DAI_amount'] = curve_df['amountSold'].where(curve_df['tokenSold'] == 'DAI').mul(-1)
    curve_df['3pool_amount'] = curve_df['amountSold'].where(curve_df['tokenSold'] == '3pool').mul(-1)
    
    curve_df['USDC_amount'] = np.where(~np.isnan(curve_df['amountBought']) & (curve_df['tokenBought'] == 'USDC'), curve_df['amountBought'], curve_df['USDC_amount'])
    curve_df['USDT_amount'] = np.where(~np.isnan(curve_df['amountBought']) & (curve_df['tokenBought'] == 'USDT'), curve_df['amountBought'], curve_df['USDT_amount'])
    curve_df['LUSD_amount'] = np.where(~np.isnan(curve_df['amountBought']) & (curve_df['tokenBought'] == 'LUSD'), curve_df['amountBought'], curve_df['LUSD_amount'])
    curve_df['DAI_amount'] = np.where(~np.isnan(curve_df['amountBought']) & (curve_df['tokenBought'] == 'DAI'), curve_df['amountBought'], curve_df['DAI_amount'])
    curve_df['3pool_amount'] = np.where(~np.isnan(curve_df['amountBought']) & (curve_df['tokenBought'] == '3pool'), curve_df['amountBought'], curve_df['3pool_amount'])
  
    curve_df = curve_df.drop(columns=['tokenSold','amountSold','tokenBought','amountBought','price'])

    return curve_df

def main(): 
    curve_df_raw = normalize_names_curve_raw(data)
    curve_df = normalize_curve(curve_df_raw).reset_index().drop(columns=['index'])
    
    print(curve_df)
    # print(curve_df)
    
    curve_df.to_feather("/Users/jonas/Workspace/Local/Drop/results/curve_logs_liquity.feather")
    print("success")
    
if __name__ == "__main__":
    main()