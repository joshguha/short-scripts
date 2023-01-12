import requests
import pandas as pd

block_start = 12399631
block_end = 16384500
num_blocks = block_end - block_start + 1

df = pd.DataFrame(columns=['blockNumber', 'LUSD_price', 'USDC_price', 'LUSD_tvl','USDC_tvl'])

for block_number in range(block_start, block_end + 1):
    query = """
    {
      pool(id: "0x4e0924d3a751be199c426d52fb1f2337fa96f736",block: {number: """ + str(block_number) + """}) {
        token0Price
        token1Price
        totalValueLockedToken0
        totalValueLockedToken1
        }
    }
    """
    # api request
    headers = {'Content-Type': 'application/json'}
    r = requests.post('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3', json={'query': query}, headers=headers)
    
    # error handling
    if r.status_code != 200:
        raise ValueError("Failed to fetch data from the Graph API")
    result = r.json()
    
    # populate dataframe
    df = df.append({'blockNumber': block_number,
                    'LUSD_price': float(result['data']['pool']['token0Price']),
                    'USDC_price': float(result['data']['pool']['token1Price']),
                    'LUSD_tvl': float(result['data']['pool']['totalValueLockedToken0']),
                    'USDC_tvl': float(result['data']['pool']['totalValueLockedToken1'])}, ignore_index=True)
    
    # progress message
    progress = 100 * (block_number - block_start + 1) / num_blocks
    print(f'Progress: {progress:.2f}%')

df.to_csv("/Users/jonas/Workspace/Local/Drop/results/liquity_univ3.csv")

print(df)