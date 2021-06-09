import boto3
import uuid
from coinbase_commerce.client import Client
import os
from datetime import datetime
from decimal import Decimal


def lambda_handler(event, context):
    print(event)
    API_KEY = os.environ["keyId"]
    client = Client(api_key=API_KEY)

    try:
        if 'sub' in event:
            sub = event['sub']
            userId = getUserId(sub)
        else:
            userId = str(uuid.uuid1())

        if 'ip' in event:
            ip = event['ip']
        else:
            ip = ""

        orderId = str(uuid.uuid1())
        print(orderId)

        discount = (Decimal(event['amountUsd']) * Decimal(event['cashBack'])).quantize(Decimal('.01'))
        finalAmount = Decimal(event['amountUsd']) - discount

        charge = client.charge.create(name=event['title'],
                                      description=event['description'],
                                      pricing_type='fixed_price',
                                      local_price={
                                          "amount": str(finalAmount),
                                          "currency": "USD"
                                      },
                                      metadata={
                                          "customer_id": orderId,
                                          "customer_name": userId

                                      },
                                      redirect_url='https://www.sudocoins.com/?msg=complete',
                                      cancel_url='https://www.sudocoins.com/?msg=cancelled')

        print(charge)

        orderRecord = {
            "userId": userId,
            "orderId": orderId,
            "statusCode": "charge:created",
            "created": datetime.utcnow().isoformat(),
            "expires": charge['expires_at'],
            "amountUsd": event['amountUsd'],
            "finalAmount": str(finalAmount),
            "productName": event['buyerName'],
            "ip": ip,
            "chargeId": charge['id'],
            "coinbase": charge,
            "email": event['email'],
            "cashBack": event['cashBack']

        }

        createOrder(orderRecord)

        response = {
            "statusCode": 200,
            "body": {
                "purchaseUrl": charge['hosted_url']
            }
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


def createOrder(orderRecord):
    dynamodb = boto3.resource('dynamodb')
    ordersTable = dynamodb.Table('orders')
    ordersTable.put_item(
        Item=orderRecord
    )


