import json
import requests
import pandas as pd
import pickle as pkl

# define vars
pool_address = "0xed279fdd11ca84beef15af5d39bb4d4bee23f0ca"
endpoint = "https://api.thegraph.com/subgraphs/name/convex-community/volume-mainnet"
PAGE_SIZE = 1000
end_block = 12399631
max_iterations = 50
counter = 0

# Define the initial query
query = """
{
  swapEvents(where: {pool: """ + '"' + pool_address + '"' + """} orderBy: block orderDirection:desc, first: """ + str(PAGE_SIZE) + """) {
    block
    timestamp
    tokenSold
    amountSold
    tokenBought
    amountBought
  }
}
"""

# Execute the initial query
response = requests.post(endpoint, json={'query': query})
data = json.loads(response.text)
found_swaps = len(data["data"]["swapEvents"])

# Initialize the last ID variable & the dataframe
last_block = data["data"]["swapEvents"][-1]["block"]
print(last_block)
df = pd.json_normalize(data['data']['swapEvents'])

# Use a while loop to repeatedly query the API if additional data in relevant time period
has_more = True
while has_more:
    try:
        if int(data["data"]["swapEvents"][0]["block"]) <= end_block or found_swaps < PAGE_SIZE:
            has_more = False
            break
        
        query = """
        {
        swapEvents(where: {pool: """ + '"' + pool_address + '"' + """, block_lt: """ + str(last_block) + """} orderBy: block orderDirection:desc, first: """ + str(PAGE_SIZE) + """) {
            id
            block
            timestamp
            tokenSold
            amountSold
            tokenBought
            amountBought
        }
        }
        """

        response = requests.post(endpoint, json={'query': query}, timeout=3)
        data = json.loads(response.text)
        found_swaps = len(data["data"]["swapEvents"])
        
        if int(data["data"]["swapEvents"][-1]["block"]) <= end_block or found_swaps < PAGE_SIZE:
            has_more = False
            break
        
        last_block = data["data"]["swapEvents"][-1]["block"]
        print(last_block)

        df = pd.concat([df, pd.json_normalize(data['data']['swapEvents'])], ignore_index=True)
        df.reset_index(drop=True,inplace=True)
        
        # iteration tracker
        counter +=1
        print(counter)
        if counter >= max_iterations:
            
            df.drop_duplicates(subset='id', keep='first', inplace=True)
            df.reset_index(inplace=True)
            
            print(len(df))
            df.to_feather('/Users/jonas/Workspace/Local/Drop/results/raw_liquity_logs_curve.feather')
            raise Exception("Maximum number of iterations reached. File saved.")

    except KeyError as e:
        print("An error occurred:", e, "File saved.")
        
        df.drop_duplicates(subset='id', keep='first', inplace=True)
        df.reset_index(inplace=True)

        print(len(df))
        df.to_feather('/Users/jonas/Workspace/Local/Drop/results/raw_liquity_logs_curve.feather')
     
# output
df.drop_duplicates(subset='id', keep='first', inplace=True)
df.reset_index(inplace=True)

print(len(df))
df.to_feather("/Users/jonas/Workspace/Local/Drop/results/raw_liquity_logs_curve.feather")
print('the loop actually exited correctly')