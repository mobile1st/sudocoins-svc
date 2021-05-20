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
        txn_hash = data['txnHash']
        transaction_id = data['transactionId']

    except Exception as e:

        return {
            "isBase64Encoded": False,
            "statusCode": 200,
            "headers": {},
            "body": 0
        }

    try:
        print("step 2")
        confirm_hash = (hashlib.md5((transaction_id + key).encode('utf-8'))).hexdigest()
        print("step 3")
        if txn_hash == confirm_hash:
            hash_state = True
            msg_value = None
            if event["queryStringParameters"]['transactionSource'] == 'iframe':
                msg_value = {
                    "queryStringParameters": event["queryStringParameters"],
                    "hashState": hash_state,
                    "buyerName": "peanutLabs"
                }

            elif event["queryStringParameters"]['transactionSource'] == 'directlink':
                msg_value = {
                    "queryStringParameters": event["queryStringParameters"],
                    "hashState": hash_state,
                    "buyerName": "dynata"
                }

            push_message(msg_value)
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


def push_message(msg_value):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    queue.send_message(MessageBody=json.dumps(msg_value), MessageGroupId='EndTransaction')
