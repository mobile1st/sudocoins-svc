import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(event)
    # body = json.loads(event['body'])

    collection_url = event.get('queryStringParameters').get('collection_url')
    log.info(collection_url)

    try:

        data = dynamodb.Table('collections').query(
            KeyConditionExpression=Key('collection_url').eq(collection_url),
            Limit=1,
            IndexName='collection_url-index',
            ProjectionExpression='more_charts'
        )

        collection_data = data['Items'][0].get('more_charts')

        return collection_data

    except Exception as e:
        log.info(f"status: failure - {e}")
        return collection_url


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))

