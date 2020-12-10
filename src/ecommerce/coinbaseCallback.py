import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    print(event)
    coinbaseEvent = event['body']['event']

    orderId = coinbaseEvent['data']['metadata']['customer_id']
    orderStatus = coinbaseEvent['type']

    updateOrder(orderId, orderStatus)

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
