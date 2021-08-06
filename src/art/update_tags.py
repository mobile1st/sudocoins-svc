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

    if 'tags' in art_record and art_record['tags'] is not None:
        tags = art_record['tags']
        for i in input_json['tags']:
            if i not in tags:
                tags.append(i)
    else:
        tags = input_json['tags']

    dynamodb.Table('art').update_item(
        Key={'art_id': input_json['art_id']},
        UpdateExpression="SET tags = :tag",
        ExpressionAttributeValues={
            ':tag': tags
        }
    )
    log.info('tags updated')

    return {
        'status': 'success',
        'message': "Tags updated"
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))