import boto3
import json
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    # input_json = json.loads(event.get('body', '{}'))
    input_json = event
    user_id = input_json.get('sub')

    # get user collections from portfolio table
    collection_list = dynamodb.Table('portfolio').query(
        KeyConditionExpression=Key('user_id').eq(user_id),
        ScanIndexForward=False,
        IndexName='user_id-index',
        ProjectionExpression='collection_code')['Items']

    log.info(collection_list)

    # get collection data for each collection

    key_list = []
    for i in collection_list:
        tmp = {
            "collection_id": i['collection_code']
        }

        key_list.append(tmp)

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, preview_url, floor, median, maximum, collection_name, chart_data'
    }
    response = dynamodb.batch_get_item(RequestItems={'collections': query})

    collections = response['Responses']['collections']

    return collections


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))

