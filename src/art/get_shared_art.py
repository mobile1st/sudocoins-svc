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

    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, #tc",
        ExpressionAttributeNames={'#n': 'name', "#tc": "contractId#tokenId"}
    )
    log.info(art_uploads_record)

    if 'Item' in art_uploads_record:
        return arts.get(art_uploads_record['Item']['art_id'])

    art = arts.get(share_id)

    if art:

        return art

    return

