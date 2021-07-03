import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    query_params = event['queryStringParameters']
    count = int(query_params['count'])
    return {
        'art': get_recent(count, query_params['timestamp'])
    }


def get_recent(count, timestamp):
    # returns recent art records paginated
    return dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
        ScanIndexForward=False,
        Limit=count,
        IndexName='Recent_index',
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, file_type, size",
        ExpressionAttributeNames={'#n': 'name'}
    )['Items']
