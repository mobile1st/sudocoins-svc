import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)

    timestamp = datetime.today().strftime('%Y-%m-%d')

    upcoming_collections = dynamodb.Table('chat').query(
        KeyConditionExpression=Key("approved").eq('true') & Key("release_date").ge(timestamp),
        ScanIndexForward=False,
        IndexName='Recent_index'
    )['Items']

    return {
        'upcoming': upcoming_collections
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))