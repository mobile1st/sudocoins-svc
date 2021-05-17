import boto3
import json
import sudocoins_logger
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('art_counter called')
    for record in event['Records']:
        payload = record['body']
        log.info(f'payload: {payload}')

        data = json.loads(payload)
        art.register_click(data)

        log.info('record updated')


