import boto3
import http.client
import json
from util import sudocoins_logger
from decimal import Decimal

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    btc_usd, eth_usd = get_coinbase_rates()

    dynamodb.Table('Config').update_item(
        Key={
            'configKey': 'HomePage'
        },
        UpdateExpression="set rate=:r, ethRate=:er",
        ExpressionAttributeValues={
            ":r": Decimal(btc_usd),
            ":er": Decimal(eth_usd)
        }
    )


def get_coinbase_rates():
    path = '/v2/exchange-rates'
    conn = http.client.HTTPSConnection('api.coinbase.com')
    conn.request('GET', path)
    response = conn.getresponse()
    rates = json.loads(response.read())
    log.info(f'rates: {rates}')
    btc_rate = rates['data']['rates']['BTC']
    eth_rate = rates['data']['rates']['ETH']
    return btc_rate, eth_rate
