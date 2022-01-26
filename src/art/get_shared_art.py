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
    share_id = event.get('pathParameters', {}).get('shareId')
    body = json.loads(event.get('body', '{}'))

    try:
        if 'collection_url' in body and 'token_id' in body:
            collection_url = body['collection_url']
            token_id = str(body['token_id'])
            collection_item_url = collection_url + "-" + token_id
            get_by_share_id(collection_item_url)
    except Exception as e:
        log.info(e)

    return get_by_share_id(share_id)


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_by_share_id(share_id):
    art = arts.get(share_id)
    if art:
        return art

    return

