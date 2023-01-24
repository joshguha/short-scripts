import os
from dotenv import load_dotenv
import pickle as pkl
from typing import Iterable, Tuple
from web3 import Web3
import json

# split block range into buckets
def generate_buckets(start: int, end: int, buckets: int) -> Iterable[Tuple[int, int]]:
    diff = int((end - start) / buckets)
    for i in range(buckets):
        bucket_0 = start + diff * i
        yield (bucket_0, bucket_0 + diff - 1)

# connect to infura
load_dotenv()
infura_id = os.getenv("INFURA_API_ID")
infura_provider = Web3.HTTPProvider("https://mainnet.infura.io/v3/"+ infura_id)
w3 = Web3(infura_provider)

# initiliaze contract
contract_address = "0x4e0924d3a751bE199C426d52fb1f2337fa96f736"
contract_abi = json.load(open('Liquity/Univ3/abi.json'))
contract = w3.eth.contract(address=contract_address, abi=contract_abi)

# use helper functions to query data while splitting queried time period into buckets to adhere to infura restrictions
def get_data(BUCKETS: int, START_BLOCK: int, END_BLOCK: int):
    # <<<change start & end block directly here>>> 
    # 12399631 -- univ3 contract deployed
    buckets = generate_buckets(START_BLOCK, END_BLOCK, BUCKETS)

    logs = []
    
    for bucket in buckets:
        print(f'Scraping blocks {bucket[0]} to {bucket[1]}')
        pass_filter = contract.events.Swap.createFilter(fromBlock=bucket[0], toBlock=bucket[1])
        logs.extend(pass_filter.get_all_entries())

    with open('liquity_logs_uniswap_test.pkl', 'wb') as handle:
        pkl.dump(logs, handle, protocol=pkl.HIGHEST_PROTOCOL)
        
    print("pickled")

if __name__ == "__main__":
    get_data(10000, 12399631, 16433095)