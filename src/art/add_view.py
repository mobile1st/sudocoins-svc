import boto3
import json
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    log.debug('add_view called')
    query_params = event['queryStringParameters']
    share_id = query_params.get('shareId')
    art_id = query_params.get('artId')

    msg = {}
    if share_id:
        msg['shareId'] = share_id
    elif art_id:
        msg['art_id'] = art_id

    log.debug(f'sending message: {msg}')
    queue = sqs.get_queue_by_name(QueueName='ArtViewCounterQueue')
    queue.send_message(MessageBody=json.dumps(msg))

    return ''
