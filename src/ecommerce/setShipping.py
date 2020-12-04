import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    print(event)
    orderDetails = updateOrder(event['body']['orderId'], event['body']['email'])

    return orderDetails


def updateOrder(orderId, email):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')

    shippingAddress = email
    shippingState = 'true'

    orderDetails = ordersTable.update_item(
        Key={
            "orderId": orderId
        },
        UpdateExpression="set shippingState=:ss, shippingAddress=:sa",
        ExpressionAttributeValues={
             ":ss": shippingState,
             ":sa": shippingAddress
        },
        ReturnValues="ALL_NEW"
    )

    return orderDetails
