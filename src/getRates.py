import boto3
import requests
from decimal import Decimal
import json


def lambda_handler(event, context):

    btcUsd, ethUsd = getCoinbaseRates()

    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table("Config")
    configTable.update_item(
            Key={
                "configKey": "HomePage"
            },
            UpdateExpression="set rate=:r, ethRate=:er",
            ExpressionAttributeValues={
                ":r": Decimal(btcUsd),
                ":er": Decimal(ethUsd)

            }
        )

    return {
        'statusCode': 200,
        'body': "Rates saved"
    }


def getCoinbaseRates():
    url = 'https://api.coinbase.com/v2/exchange-rates'

    response = requests.get(url)
    rates = json.loads(response.content.decode("utf-8"))
    btcRate = rates['data']['rates']['BTC']
    ethRate = rates['data']['rates']['ETH']

    return btcRate, ethRate



