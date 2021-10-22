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


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_by_share_id(source_ip, share_id, user_id):
    msg = {'sourceIp': source_ip}
    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue.fifo')
    # queue deduplication by sourceIp+artId/shareId for 5 minutes

    art_uploads_record = dynamodb.Table('art_uploads').get_item(
        Key={'shareId': share_id},
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, contractId#tokenId",
        ExpressionAttributeNames={'#n': 'name'}
    )
    log.info(art_uploads_record)

    if 'Item' in art_uploads_record:
        msg['shareId'] = share_id
        log.info(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        return arts.get(art_uploads_record['Item']['art_id'])

    art = arts.get(share_id)
    if art:
        msg['art_id'] = share_id
        log.debug(f'sending message: {msg}')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='share_views')
        return art

    return

