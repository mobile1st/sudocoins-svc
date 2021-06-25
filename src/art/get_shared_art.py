import boto3
import json
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')
# remove prompt and art

def lambda_handler(event, context):
    log.info(f'art_prompt {event}')

    share_id = event['pathParameters']['shareId']
    source_ip = event['requestContext']['http']['sourceIp']
    try:
        query_params = event['queryStringParameters']
        unique_id = query_params.get('userId')
        if unique_id:
            print("true")
            user_id = unique_id
        else:
            user_id = source_ip
    except Exception as e:
        user_id = source_ip


    return get_by_share_id(source_ip, share_id, user_id)


def get_by_share_id(source_ip, share_id, user_id):
    # returns the art_uploads record based on shareId
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count",
        ExpressionAttributeNames={'#n': 'name'}
    )

    print(art_uploads_record)

    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue.fifo')
    # queue deduplication by sourceIp+artId/shareId for 5 minutes
    msg = {'sourceIp': source_ip}
    if 'Item' in art_uploads_record:
        msg['shareId'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')

        return {
            "art_id": art_uploads_record['Item']['art_id'],
            "click_count": art_uploads_record['Item']['click_count'],
            "name": art_uploads_record['Item']['name'],
            "art_url": art_uploads_record['Item']['art_url'],
            "preview_url": art_uploads_record['Item']['preview_url']
            }

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url",
        ExpressionAttributeNames={'#n': 'name'})

    if 'Item' in art_record:
        msg['art_id'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')

        return {
            "art_id": art_record['Item']['art_id'],
            "click_count": art_record['Item']['click_count'],
            "name": art_record['Item']['name'],
            "art_url": art_record['Item']['art_url'],
            "preview_url": art_record['Item']['preview_url']
            }

    return

