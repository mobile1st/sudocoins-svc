import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    input_json = json.loads(event.get('body', '{}'))

    art_record = dynamodb.Table('art').get_item(
        Key={'art_id': input_json['art_id']}
    )['Item']

    if art_record['first_user'] == input_json['user_id']:
        dynamodb.Table('art').update_item(
            Key={'art_id': input_json['art_id']},
            UpdateExpression="SET tags = :tag",
            ExpressionAttributeValues={
                ':tag': input_json['tags']
            }
        )
        log.info('tags updated')
        return
    else:
        log.info('user not authorized to update tags')
        return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))