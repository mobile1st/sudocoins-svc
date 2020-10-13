import json
import os
import boto3


def lambda_handler(event, context):
    # TODO implement Kinesis writes
    try:
        params = event["queryStringParameters"]
        kinesis = boto3.client('kinesis')
        item = {
            "status": params["status"],
            "IP address": params["IP address"],
            "transaction_timestamp": params["transaction_timestamp"],
            "signature_hmac_sha": params["signature_hmac_sha"]}

        try:
            """
            kinesis.put_record(StreamName=os.environ["KINESIS_STREAM"],
                               Data=json.dumps(item),
                               PartitionKey=str(params["transaction_id"]))"""

            return {
                'statusCode': 200,
                'body': 'Configure writes for Kineses'
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


