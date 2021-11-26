import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
from datetime import datetime
import operator

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)

    timestamp = str(datetime.today().strftime('%Y-%m-%d'))

    upcoming_collections = dynamodb.Table('upcoming').query(
        KeyConditionExpression=Key("approved").eq('true') & Key("release_date").gte(timestamp),
        ScanIndexForward=False,
        IndexName='approved-index'
    )['Items']

    s = sorted(upcoming_collections, key=lambda x: (x['release_date'], x['release_time']))

    return {
        'upcoming': s
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))