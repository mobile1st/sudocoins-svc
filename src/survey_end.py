import json
import os
import boto3


def lambda_handler(event, context):
    try:
        params = event["queryStringParameters"]
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
        item = {
            "transaction_id": params["transaction_id"],
            "status": params["status"],
            "IP address": params["IP address"],
            "transaction_timestamp": params["transaction_timestamp"],
            "signature_hmac_sha": params["signature_hmac_sha"]}
        try:
            response = queue.send_message(MessageBody=json.dumps(item), MessageGroupId='cint')
            return {
                'statusCode': 200,
                'body': 'Success'
            }

        except Exception as e:
            return {
                "status": 400,
                "body": "Invalid kinesis record"
            }

    except Exception as e:
        print(e)
        return {
            "status": 400,
            "body": "Bad Request"
        }


