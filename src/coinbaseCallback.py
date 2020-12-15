import boto3
import json
import os
import hmac
import hashlib
import history


def lambda_handler(event, context):
    print("start function")
    print(event)
    dynamodb = boto3.resource('dynamodb')
    order = history.History(dynamodb)

    try:
        state = checkSig(event)
        if state:
            pass

        else:
            return {
                "statusCode": 200,
                "body": "bad signature"
            }

    except Exception as e:
        print(e)

        return {
            "statusCode": 200,
            "body": "bad signature"
        }

    try:

        coinbase = json.loads(event['body'])
        coinbaseEvent = coinbase['event']

        orderId = coinbaseEvent['data']['metadata']['customer_id']
        orderStatus = coinbaseEvent['type']
        '''
        orderId = "3d17ee4a-25-11eb-8acc-6542e93cfe93"
        orderStatus = 'charge:confirmed'
        '''
        print("about to update order")
        order.updateOrder(orderId, orderStatus)

    except Exception as e:
        print(e)

    return {
        "statusCode": 200,
        "body": "success"
    }


'''

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
'''


def checkSig(event):
    header = event['headers']['X-Cc-Webhook-Signature']
    secret = os.environ["key"].encode('utf-8')
    message = event['body'].encode('utf-8')
    signature = hmac.new(
        secret,
        msg=message,
        digestmod=hashlib.sha256
    ).hexdigest()
    if header == signature:
        return True
    else:
        return False
