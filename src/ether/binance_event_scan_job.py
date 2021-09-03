import os
import json
import boto3
import asyncio
from web3 import Web3
from threading import Timer
from util import sudocoins_logger

# infra_url as etherum relay service
ABI = json.loads('[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"spender","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Paused","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":false,"internalType":"uint256","name":"value","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Unpaused","type":"event"},{"inputs":[],"name":"INITIAL_SUPPLY","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"spender","type":"address"}],"name":"allowance","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"approve","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"account","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"burnFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"subtractedValue","type":"uint256"}],"name":"decreaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"spender","type":"address"},{"internalType":"uint256","name":"addedValue","type":"uint256"}],"name":"increaseAllowance","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"owner","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pause","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"renounceOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transfer","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"sender","type":"address"},{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount","type":"uint256"}],"name":"transferFrom","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"unpause","outputs":[],"stateMutability":"nonpayable","type":"function"}]')

# Rarible smart contract abi & address
ADDRESS = "0xde3dbBE30cfa9F437b293294d1fD64B26045C71A"
ANKR_URL = "https://teoh:Teoh$123@apis-sj.ankr.com/34af2d7b629640b189f497dfed490c37/a5cfb7095fa06d34d977d4faa846e49f/binance/full/main"

# initialize web3 by provider
web3 = Web3(Web3.HTTPProvider(ANKR_URL))

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
binance_events_table = dynamodb.Table('binance_events')



def lambda_handler(event, context):
    #Logging web3 connection
    log.debug(web3.isConnected())
    loop = asyncio.get_event_loop()
    contract = get_smart_contract(ADDRESS, ABI)
    eventFilter = create_event_filter(contract)
    group = asyncio.gather(event_loop(eventFilter, 10))
    # timer = Timer(59.0, closeLoop, {group})
    # timer.start()
    try:
        loop.run_until_complete(group)
    finally:
        loop.close()

# Get Contract from EtherScan
def get_smart_contract(address, abi):
    contract = web3.eth.contract(address=address, abi=abi)
    return contract


# Create Filter for fetch events
def create_event_filter(contract):
    eventFilter = contract.events.Transfer.createFilter(fromBlock="latest")
    return eventFilter;

async def event_loop(filter, interval):
    while True:
        for event in filter.get_new_entries():
            save_event(event)
        await asyncio.sleep(interval)

def closeLoop(group):
    group.cancel()

# Save event
def save_event(event):
    if event is not None:
        eventData = {
            "transactionIndex": event['transactionIndex'],
            "from": event['args']['from'],
            "to": event['args']['to'],
            "tx_hash": web3.toHex(event['transactionHash']),
            "blockNumber": event['blockNumber'],
            "blockHash": web3.toHex(event['blockHash']),
            "value": event['args']['value']
        }
        try:
            binance_events_table.put_item(
                Item=eventData
            )
        except Exception as e:
            log.exception(e)
    else:
        log.debug("There is no new Event")