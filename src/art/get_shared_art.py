import boto3
import json
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'art_prompt {event}')
    share_id = event['pathParameters']['shareId']

    return get_by_share_id(share_id)


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_by_share_id(share_id):
    art = arts.get(share_id)
    if art:
        return art

    return

