import boto3
import json
from transaction import Transaction

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")
transaction = Transaction(dynamodb, sns_client)


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            payload = record['body']
            print(payload)

            data = json.loads(payload)
            transaction.end(data)
            print("record updated")

        return {
            "status": 200,
            "body": "Success! Records pulled from queue and updated"
        }

    except Exception as e:
        print(e)

        return {
            "status": 200,
            "body": "Failure! Something went wrong"
        }
