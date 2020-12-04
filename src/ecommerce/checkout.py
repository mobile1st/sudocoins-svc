import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    print(event)

    try:

        if 'sub' in event:
            sub = event['sub']
            userId = getUserId(sub)
            if 'sourceIp' in event['requestContext']['identity']:
                ip = 'sourceIp' in event['requestContext']['identity']
        else:
            if 'sourceIp' in event['requestContext']['identity']:
                userId = event['requestContext']['identity']['sourceIp']
                ip = event['requestContext']['identity']['sourceIp']
            else:
                userId = str(uuid.uuid1())
                ip = ""

            orderId = str(uuid.uuid1())

            wyreBody = {
                "userId": userId,
                "orderId": orderId,
                "currency": event['body']['currency']
            }

            # methond to Wyre queue to create wallet

            created = str(datetime.utcnow().isoformat())

            orderRecord = {
                "userId": userId,
                "orderId": orderId,
                "statusCode": 2,
                "statusMessage": "Payment needed",
                "created": created,
                "started": created,
                "end": created + datetime.timedelta(minutes=15),
                "amountUsd": event['body']['amountUsd'],
                "amountCurrency": event['body']['amountCurrency'],  # or we calculate
                "currency": event['body']['amountCurrency'],
                "rate": event['body']['rate'],
                "shippingState": "false",
                "shippingAddress": ""

            }

            # put order record in DB

            response = {
                "statusCode": 200,
                "body": "success"
            }

            return response


    except Exception as e:
        print(e)
        response = {
            "statusCode": 200,
            "body": "error"
        }

        return response


def getUserId(sub):
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        print("founder userId matching sub")
        userId = subResponse['Item']['userId']

        return userId
