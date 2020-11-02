import boto3
import json
import uuid
from datetime import datetime
import requests


def lambda_handler(event, context):
    """gets exchange rates and saves them to cache
    Arguments: none
    Returns: rates saved
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("exchangeRates")

    btcUsd = getBtc()

    data = {
        "currency": "usd",
        "btcUsd": str(btcUsd),
        "btcSudo": str(btcUsd * 100)
    }

    ratesResponse = profileTable.put_item(
        Item=data
    )


def getBtc():
    url = 'https://blockchain.info/tobtc?'
    params = {
        "currency": "USD",
        "value": 1
    }
    response = requests.get(url, params=params)
    btcRate = response.content.decode("utf-8")

    return btcRate