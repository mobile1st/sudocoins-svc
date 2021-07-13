import boto3
import json
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    body = json.loads(event['body'])
    arts = body['arts']
    if len(arts) == 0:
        return {'arts': None}

    return {
        'arts': art.get_arts(arts)
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


