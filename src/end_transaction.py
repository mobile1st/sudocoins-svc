import boto3
import json
from transaction import Transaction


def lambda_handler(event, context):
    try:
        for record in event['Records']:
            payload = record['body']
            print(payload)

            update(payload)
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


def update(payload):
    data = json.loads(payload)
    dynamodb = boto3.resource('dynamodb')
    transaction = Transaction(dynamodb)

    if data["buyerName"] == 'cint' or data["buyerName"] == 'test':
        transaction.endTest(data)

    elif data["buyerName"] == 'lucid':
        transaction.endLucid(data)

    elif data["buyerName"] == 'peanutLabs':
        transaction.endPL(data)

    elif data["buyerName"] == 'dynata':
        transaction.endDynata(data)

