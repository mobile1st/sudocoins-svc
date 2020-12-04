import boto3
import json


def lambda_handler(event, context):

    try:
        buyerName = event['pathParameters']['buyerName']
        dynamodb = boto3.resource('dynamodb')
        buyer, rates = getConfig(dynamodb, buyerName)
        giftCard = {
            "buyer": buyer,
            "rates": rates
        }
        response = {
            "statusCode": 200,
            "body": json.dumps(giftCard)
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
    rates = config['rates']

    return buyer, rates