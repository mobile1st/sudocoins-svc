import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from collections import OrderedDict
from operator import getitem

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    period = (datetime.utcnow() - timedelta(days=1)).isoformat()
    trending_art = get_trending(period)
    arts = []
    for i in trending_art:
        arts.append(i['art_id'])

    set_config(arts[:250])

    return {
        'trending': arts[:250]
    }



def get_trending(period):

    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price"
    )

    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['last_sale_price'], reverse=True)

    return sorted_arts



def set_config(arts):
    config_table = dynamodb.Table('Config')
    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art",
        ExpressionAttributeValues={
            ":art": arts
        },
        ReturnValues="ALL_NEW"
    )
    log.info(f'updated_art {updated_art}')
