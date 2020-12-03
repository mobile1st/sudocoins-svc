import boto3
import requests
from decimal import Decimal
from ecommerce.wyre import Wyre
import json


def lambda_handler(event, context):

    btcUsd = getBtc()
    wyreRates = getWyreRates()

    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table("Config")
    configTable.update_item(
            Key={
                "configKey": "TakeSurveyPage"
            },
            UpdateExpression="set rate=:r, rates=:rs",
            ExpressionAttributeValues={
                ":r": Decimal(btcUsd),
                ":rs": wyreRates

            }
        )

    return {
        'statusCode': 200,
        'body': "Rates saved"
    }


def getBtc():
    url = 'https://blockchain.info/tobtc?'
    params = {
        "currency": "USD",
        "value": 1
    }
    response = requests.get(url, params=params)
    btcRate = response.content.decode("utf-8")

    return btcRate


def getWyreRates():
    wyre = Wyre()
    wyreRates = json.loads(wyre.getRates().text.encode('utf8'))
    print(wyreRates)
    rates = {}
    currencies = {
        "BTC": "Bitcoin",
        "ETH": "Ethereum",
        "DAI": "DAI",
        "USDC": "USD Coin",
        "USDT": "Tether",
        "BUSD": "Binance USD",
        "GUSD": "Gemini Dollar",
        "PAX": "Paxos Standard",
        "USDS": "Stably Dollar",
        "AAVE": "Aave",
        "COMP": "Compound",
        "LINK": "Chainlink",
        "WBTC": "Wrapped Bitcoin",
        "BAT": "Basic Attention Token",
        "CRV": "Curve",
        "MKR": "Maker",
        "SNX": "Synthetix",
        "UMA": "UMA",
        "UNI": "Uniswap",
        "YFI": "yearn.finance"
        }

    for i in currencies:
        tmp = "USD" + i
        print(tmp)
        rates[i] = {
            "rate": str(wyreRates[tmp][i]),
            "name": currencies[i]
        }


    return rates


'''
exchangeRatesTable = dynamodb.Table("exchangeRates")

    btcRates = {
        "currency": "btc",
        "sudo": Decimal(btcUsd) * Decimal('.01'),
        "usdBtc": Decimal(btcUsd),
        "precision": 8
    }

    usdRates = {
        "currency": "usd",
        "sudo": Decimal('.01'),
        "usdBtc": Decimal(btcUsd),
        "precision": 2
    }
exchangeRatesTable.put_item(
    Item=btcRates
)

exchangeRatesTable.put_item(
    Item=usdRates
)
'''