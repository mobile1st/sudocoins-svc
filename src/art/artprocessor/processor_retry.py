import boto3
import json
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    return
    set_log_context(event)
    log.debug(f'add_view event{event}')

    art_records = dynamodb.Table('art').query(
        KeyConditionExpression=Key("process_status").eq("STREAM_TO_S3"),
        IndexName='process_status_index',
        ProjectionExpression="art_id, open_sea_data")['Items']

    art_list = []
    for i in art_records:
        msg = {
            "art_url": i['open_sea_data']['image_url'],
            "art_id": i['art_id']
        }

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
                },
                'process': {
                    'DataType': 'String',
                    'StringValue': 'STREAM_TO_S3'
                }
            },
            Message=json.dumps(msg)
        )
        art_list.append(i['art_id'])

    log.info("arts pushed to sns")
    log.info(art_list)


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
