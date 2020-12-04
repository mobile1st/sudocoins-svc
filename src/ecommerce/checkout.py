import boto3
from datetime import datetime
import uuid
from decimal import *
import json


def lambda_handler(event, context):
    print(event)

    try:
        if 'sub' in event:
            sub = event['sub']
            userId = getUserId(sub)
            if 'sourceIp' in event['requestContext']['identity']:
                ip = event['requestContext']['identity']
        else:
            if 'sourceIp' in event['requestContext']['identity']:
                userId = event['requestContext']['identity']['sourceIp']
                ip = event['requestContext']['identity']['sourceIp']
            else:
                userId = str(uuid.uuid1())
                ip = ""

        orderId = str(uuid.uuid1())
        created = str(datetime.utcnow().isoformat())
        amountCurrency = ((Decimal(event['body']['amountUsd'])) * (Decimal(event['body']['rate']))).quantize(
            Decimal('10') ** ((-1) * int(8)))

        orderRecord = {
            "userId": userId,
            "orderId": orderId,
            "statusCode": 2,
            "statusMessage": "Payment needed",
            "created": created,
            "started": created,
            "end": created + datetime.timedelta(minutes=15),
            "amountUsd": event['body']['amountUsd'],
            "amountCurrency": amountCurrency,
            "currency": event['body']['currency'],
            "rate": event['body']['rate'],
            "shippingState": "false",
            "shippingAddress": "",
            "ip": ip

        }

        wyreBody = {
            "orderId": orderId,
            "currency": event['body']['currency']
        }

        createOrder(orderRecord)
        pushMsg(wyreBody)

        response = {
            "statusCode": 200,
            "body": "Order created. Redirect to Payment page."
        }

        return response

    except Exception as e:
        print(e)
        response = {
            "statusCode": 200,
            "body": "Error. Show user error message."
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


def pushMsg(msgValue):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='createWallet.fifo')
    queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='newWallet')


def createOrder(orderRecord):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')
    ordersTable.put_item(
        Item=orderRecord
    )


