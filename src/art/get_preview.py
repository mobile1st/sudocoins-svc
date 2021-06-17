import boto3
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.info(f'get_preview {event}')

    return
