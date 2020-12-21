import json
import hashlib
import boto3
import os


def lambda_handler(event, context):
    print(event)
    key = os.environ["keyId"]
    print(key)

    try:
        data = event['queryStringParameters']
        txnHash = data['txnHash']
        transactionId = data['transactionId']

    except Exception as e:

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {},
            "body": 0
        }

    try:
        print("step 2")
        confirmHash = (hashlib.md5((transactionId + key).encode('utf-8'))).hexdigest()
        print("step 3")
        if txnHash == confirmHash:
            hashState = True

            if event["queryStringParameters"]['transactionSource'] == 'iframe':
                msgValue = {
                    "queryStringParameters": event["queryStringParameters"],
                    "hashState": hashState,
                    "buyerName": "peanutLabs"
                }

            elif event["queryStringParameters"]['transactionSource'] == 'directlink':
                msgValue = {
                    "queryStringParameters": event["queryStringParameters"],
                    "hashState": hashState,
                    "buyerName": "dynata"
                }

            pushMsg(msgValue)
            print("message pushed")

            return {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": {},
                "body": 1
            }
        else:
            print("step 4")
            return {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": {},
                "body": 0
            }

    except Exception as e:

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {},
            "body": 0
        }


def pushMsg(msgValue):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')

    return record


def missingParams(params, expectedParams):
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)
    return missingParams


def getExpectedParams():
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table('Config')
    configKey = "dynataRedirect"

    response = configTable.get_item(Key={'configKey': configKey})
    expectedParams = response['Item']["configValue"]["expectedParams"]

    return expectedParams
