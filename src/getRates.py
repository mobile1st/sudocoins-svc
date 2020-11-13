import boto3
import json
import uuid
from datetime import datetime
import requests
from decimal import Decimal


# from .exchange_rates import ExchangeRates


def lambda_handler(event, context):
    # todo use exchange rates
    # dynamodb = boto3.resource('dynamodb')
    # exchange = ExchangeRates(dynamodb)
    # exchange.refresh_rate('btc')
    #
    # return {'statusCode': 200}
    """gets exchange rates and saves them to cache
    Arguments: none
    Returns: rates saved
    """
    dynamodb = boto3.resource('dynamodb')
    exchangeRatesTable = dynamodb.Table("exchangeRates")

    btcUsd = getBtc()

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

    btcRatesResponse = exchangeRatesTable.put_item(
        Item=btcRates
    )

    usdRatesResponse = exchangeRatesTable.put_item(
        Item=usdRates
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
