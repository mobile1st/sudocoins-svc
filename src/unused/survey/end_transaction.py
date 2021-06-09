import boto3
import json
from util import sudocoins_logger
from survey.transaction import Transaction

log = sudocoins_logger.get()

dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')
transaction = Transaction(dynamodb, sns_client)


def lambda_handler(event, context):
    log.debug('end_transaction called')
    for record in event['Records']:
        payload = record['body']
        log.info(f'payload: {payload}')
        try:
            data = json.loads(payload)
            transaction.end(data)
            log.info('record updated')
        except Exception:
            log.exception(f'Could not end transaction for payload={payload}')
