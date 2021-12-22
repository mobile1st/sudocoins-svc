import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
from datetime import datetime
import operator
import json
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)

    timestamp = str(datetime.today().strftime('%Y-%m-%d'))

    body = json.loads(event['body'])
    if body.get('action') is None:

        upcoming_collections = dynamodb.Table('upcoming').query(
            KeyConditionExpression=Key("approved").eq('true') & Key("release_date").gte(timestamp),
            ScanIndexForward=False,
            IndexName='approved-index'
        )['Items']

        next_collection = []

        for i in upcoming_collections:
            if i['release_date'] is None:
                i['release_date'] = ""
            if i['release_time'] is None:
                i['release_date'] = ""

            next_collection.append(i)

        s = sorted(next_collection, key=lambda x: (x.get('release_date', ""), x.get('release_time', "")))

        return {
            'upcoming': s
        }

    else:
        time_window = (datetime.fromisoformat(timestamp) - timedelta(days=14)).isoformat()
        upcoming_collections = dynamodb.Table('upcoming').query(
            KeyConditionExpression=Key("approved").eq('true') & Key("release_date").between(time_window, timestamp),
            ScanIndexForward=False,
            IndexName='approved-index'
        )['Items']

        next_collection = []

        for i in upcoming_collections:
            if i['release_date'] is None:
                i['release_date'] = ""
            if i['release_time'] is None:
                i['release_date'] = ""

            next_collection.append(i)

        s = sorted(next_collection, key=lambda x: (x.get('release_date', ""), x.get('release_time', "")))

        return {
            'upcoming': s
        }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))