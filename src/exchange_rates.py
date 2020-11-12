import requests
from decimal import Decimal


class ExchangeRates:
    def __init__(self, dynamodb):
        self.dynamodb = dynamodb

    def get_rate(self, currency):
        exchange = self.dynamodb.Table("exchangeRates")
        row = exchange.get_item(Key={'currency': currency})
        return Decimal(row['Item']["sudo"]), row['Item']["precision"]

    def refresh_rate(self, currency):
        rate = self.fetch_rate(currency)

        exchange = self.dynamodb.Table("exchangeRates")
        exchange.put_item(Item={
            "currency": currency,
            "sudo": str(rate)
        })

    @staticmethod
    def fetch_rate(currency):
        if currency.lower() == "btc":
            url = 'https://blockchain.info/tobtc?'
            response = requests.get(url, params={
                "currency": "USD",
                "value": 0.01
            })
            return response.content.decode("utf-8")
        elif currency.lower() == "usd":
            return 0.01  # sudo coin is pinned to the cent
        else:
            raise RuntimeError('Unsupported exchange for currency ' + currency)
