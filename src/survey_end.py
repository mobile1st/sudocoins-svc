import json
import os
import boto3


def lambda_handler(event):
    # TODO implement
    try:
        params = event["queryStringParameters"]
        kinesis = boto3.client('kinesis')
        item = {
            "status": params["status"],
            "IP address": params["IP"],
            "transaction_timestamp": params["transaction_timestamp"],
            "signature_hmac_sha": params["signature_hmac_sha"],
        }
        try:
            kinesis.put_record(StreamName=os.environ["KINESIS_STREAM"],
                               Data=json.dumps(item),
                               PartitionKey=str(params["transaction_id"]))
            return {
                'statusCode': 200,
                'body': 'Succeed!'
            }
        except Exception as e:
            print(f"kinesis put_record: {e}")
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
