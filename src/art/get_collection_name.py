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

    if 'collection_id' in body:
        return {
            'collection_data': get_collection2(body['collection_id'])['collection_data']
        }

    collection = body['collectionId']

    return {
        'collection_data': get_collection(collection)['collection_data']
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_collection(collection):

    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_address').eq(collection),
        ScanIndexForward=False,
        Limit=1,
        IndexName='collection_address-recent_sk-index',
        ProjectionExpression='collection_data'
    )

    collection_data = data['Items'][0]

    return collection_data


def get_collection2(collection_id):

    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        Limit=1,
        IndexName='collection_id-recent_sk-index',
        ProjectionExpression='collection_data'
    )

    collection_data = data['Items'][0]

    return collection_data
