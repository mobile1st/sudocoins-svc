import boto3
from datetime import datetime
import uuid
from decimal import *


def lambda_handler(event, context):
    print(event)

    # parse event. Will it provide walletId or srn? Do we need to do a lookup?
    # save status
    # updateOrder(status)

    return {
        "statusCode": 200,
        "body": "success"
    }


def updateOrder(status):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')
    '''
    ordersTable.update_item(
        Key={
            "orderId": orderId
        },
        UpdateExpression="set walletId=:wid, walletAdrress=:wad",
        ExpressionAttributeValues={
            ":wid": walletId,
            ":wad": walletAddress
        },
        ReturnValues="ALL_NEW"
    )
    '''