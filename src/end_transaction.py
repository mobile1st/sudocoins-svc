import boto3
import json
import logging
from transaction import Transaction

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')
transaction = Transaction(dynamodb, sns_client)


def lambda_handler(event, context):
    failures = []
    for record in event['Records']:
        payload = record['body']
        print(payload)
        try:
            data = json.loads(payload)
            transaction.end(data)
            print('record updated')
        except Exception:
            failures.append(payload)
            logging.exception(f'Could not end transaction for payload={payload}')

    if len(failures) > 0:
        return {
            'status': 200,
            'body': f'Could not end transaction for events={failures}'
        }
    return {
        'status': 200,
        'body': 'Transaction successfully ended'
    }
