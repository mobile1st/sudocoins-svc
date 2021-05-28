import boto3
import json
import sudocoins_logger
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('add_view called')

    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='art_counter.fifo')

    if 'art_id' in event:
        msgValue = {
            'art_id': event['art_id']
        }

    elif 'shareId' in event:
        msgValue = {
            'shareId': event['shareId']
        }

    queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='tile_views')

    return {
        'statusCode': 200
    }

