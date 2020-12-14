import json
import os
import boto3
from decimal import Decimal
import hmac
import hashlib
import base64


def lambda_handler(event, context):
    print(event)
    params = event["queryStringParameters"]
    status = event['pathParameters']['status']
    url = ""  # . how do we get the Url?

    lucidHash = params['hash']
    hashState = checkHash(url, lucidHash)
    if hashState:
        pass
    else:
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/invalid'},
            "body": json.dumps({})
        }

        return response

    if status == 'failure':
        msg = {
            "userId": params['pid'],
            "transactionId": params['mid'],
            "hashState": hashState,
            "buyerName": "lucid",
            "path": status,
            "queryStringParameters": params
        }
        pushMsg(msg)

        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/valid'},
            "body": json.dumps({})
        }

        return response

    elif status == 'success':
        msg = {
            "userId": params['pid'],
            "transactionId": params['mid'],
            "surveyId": params['sur'],
            "revenue": params['c'],
            "surveyLoi": params['l'],
            "hashState": hashState,
            "buyerName": "lucid",
            "path": status,
            "queryStringParameters": params,
            "sudoCut": Decimal(params['c']) * Decimal('.7'),
            "userCut": (Decimal(params['c']) * Decimal('.7')) * Decimal('.8')
        }

        pushMsg(msg)

        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/valid'},
            "body": json.dumps({})
        }

        return response

    else:
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/invalid'},
            "body": json.dumps({})
        }

        return response


def checkHash(url, lucidHash):
    key = os.environ["key"]
    encoded_key = key.encode('utf-8')
    encoded_URL = url.encode('utf-8')
    hashed = hmac.new(encoded_key, msg=encoded_URL, digestmod=hashlib.sha1)
    digested_hash = hashed.digest()
    base64_encoded_result = base64.b64encode(digested_hash)
    final_result = base64_encoded_result.decode('utf-8').replace('+', '-').replace('/', '_').replace('=', '')

    if final_result == lucidHash:
        return True
    else:
        return False


def pushMsg(msgValue):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    record = queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')

    return record
