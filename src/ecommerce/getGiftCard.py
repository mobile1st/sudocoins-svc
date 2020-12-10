import boto3
import json


def lambda_handler(event, context):

    try:
        buyerName = event['buyerName']
        print(buyerName)
        dynamodb = boto3.resource('dynamodb')
        buyer, currencies = getConfig(dynamodb, buyerName)
        giftCard = {
            "name": buyer['name'],
            "description": buyer["description"],
            "title": buyer['productTitle'],
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
        print(e)
        response = {
            "statusCode": 200,
            "body": "No buyer with name" + event['buyerName']
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