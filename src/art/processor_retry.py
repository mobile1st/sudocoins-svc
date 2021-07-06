import boto3
import json
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'add_view event{event}')

    art_records = dynamodb.Table('art').query(
        KeyConditionExpression=Key("process_status").eq("attempted"),
        IndexName='process_status-index',
        ProjectionExpression="art_id, open_sea_data")['Items']

    art_list = []
    for i in art_records:
        msg = {
            "art_url": i['open_sea_data']['image_url'],
            "art_id": i['art_id']
        }

        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
            MessageStructure='string',
            MessageAttributes={
                'art_id': {
                    'DataType': 'String',
                    'StringValue': i['art_id']
                },
                'art_url': {
                    'DataType': 'String',
                    'StringValue': i['open_sea_data']['image_url']
                }
            },
            Message=json.dumps(msg)
        )
        art_list.append(i['art_id'])

    log.info("arts pushed to sns")
    log.info(art_list)

    return ''


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
