import boto3
import json


def lambda_handler(event, context):

    try:
        buyerName = event['buyerName']
        dynamodb = boto3.resource('dynamodb')
        buyer, currencies = getConfig(dynamodb, buyerName)
        giftCard = {
            "name": buyer['name'],
            "description": buyer["title"],
            "type": buyer['type'],
            "amounts": buyer['amounts'],
            "currencies": currencies
        }
        response = {
            "statusCode": 200,
            "body": giftCard
        }

        return response

    except Exception as e:
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response


def getConfig(dynamodb, buyerName):
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"
    response = configTable.get_item(Key={'configKey': configKey})

    config = response['Item']
    buyer = config['configValue']['buyers'][buyerName]
    rates = config['currencies']

    return buyer, rates