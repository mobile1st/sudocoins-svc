import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    log.debug(f'art_prompt {event}')
    share_id = event['rawPath'].replace('/art/share/', '')
    source_ip = event['requestContext']['http']['sourceIp']
    return get_by_share_id(source_ip, share_id)


def get_by_share_id(source_ip, share_id):
    # returns the art_uploads record based on shareId
    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count",
        ExpressionAttributeNames={'#n': 'name'}
    )

    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue.fifo')
    # queue deduplication by sourceIp+artId/shareId for 5 minutes
    msg = {'sourceIp': source_ip}
    if 'Item' in art_uploads_record:
        msg['shareId'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        return art_uploads_record['Item']

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, buy_url",
        ExpressionAttributeNames={'#n': 'name'})

    if 'Item' in art_record:
        msg['art_id'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        return art_record['Item']

    return {
        "message": "art not found"
    }
