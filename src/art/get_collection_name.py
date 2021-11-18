import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    body = json.loads(event['body'])

    collection_id = body['collection_id']

    try:

        res = dynamodb.Table('collections').get_item(
            Key={'collection_id': collection_id},
            ProjectionExpression="collection_data")

        if 'Item' in res and 'collection_data' in res['Item']:

            return{
                "collection_data": res['Item']['collection_data']
            }

    except Exception as e:
        log.info(e)


    return {
        'collection_data': get_collection(collection_id)['collection_data']
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_collection(collection_id):

    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        Limit=1,
        IndexName='collection_id-event_date-index',
        ProjectionExpression='collection_data'
    )

    collection_data = data['Items'][0]

    return collection_data
