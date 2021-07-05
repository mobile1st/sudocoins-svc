import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'add_view event{event}')
    query_params = event['queryStringParameters']
    share_id = query_params.get('shareId')
    art_id = query_params.get('artId')

    # queue deduplication by sourceIp+artId/shareId for 5 minutes
    msg = {'sourceIp': event['requestContext']['http']['sourceIp']}
    if share_id:
        msg['shareId'] = share_id
    elif art_id:
        msg['art_id'] = art_id

    log.debug(f'sending message: {msg}')
    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue.fifo')
    queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='tile_views')

    return ''


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
