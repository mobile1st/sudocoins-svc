import boto3
from datetime import datetime
import uuid
from decimal import *
import json


def lambda_handler(event, context):
    print(event)
    try:
        coinbase = json.loads(event['body'])
        print(coinbase)
        coinbaseEvent = coinbase['event']
        print(coinbaseEvent)

        orderId = coinbaseEvent['data']['metadata']['customer_id']
        orderStatus = coinbaseEvent['type']

        updateOrder(orderId, orderStatus)

    except Exception as e:
        print(e)

    return {
        "statusCode": 200,
        "body": "success"
    }


def updateOrder(orderId, orderStatus):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')

    ordersTable.update_item(
        Key={
            "orderId": orderId
        },
        UpdateExpression="set statusCode=:sc",
        ExpressionAttributeValues={
            ":sc": orderStatus
        },
        ReturnValues="ALL_NEW"
    )
