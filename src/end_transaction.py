import boto3
import json
import sudocoins_logger
from transaction import Transaction

log = sudocoins_logger.get(__name__)

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')
transaction = Transaction(dynamodb, sns_client)


def lambda_handler(event, context):
    for record in event['Records']:
        payload = record['body']
        log.info(f'payload: {payload}')
        try:
            data = json.loads(payload)
            transaction.end(data)
            log.info('record updated')
        except Exception:
            log.exception(f'Could not end transaction for payload={payload}')
