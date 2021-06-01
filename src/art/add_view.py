import boto3
import json
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    log.debug('add_view called')
    queue = sqs.get_queue_by_name(QueueName='art_counter.fifo')

    msg = {}
    path = event['rawPath'].replace('/increment', '')
    if '/art/share/' in path:
        msg['shareId'] = path.replace('/art/share/', '')
    else:
        msg['art_id'] = path.replace('/art/', '')

    queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='tile_views')

    return ''
