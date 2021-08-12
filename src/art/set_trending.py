import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    trending_hour = get_trending("hours", 1)
    trending_day = get_trending("days", 1)
    trending_week = get_trending("days", 7)

    set_config(trending_hour, trending_day, trending_week)

    return


def set_config(hour, day, week):
    config_table = dynamodb.Table('Config')
    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art, trending_day=:day, trending_week=:week",
        ExpressionAttributeValues={
            ":art": hour,
            ":day": day,
            ":week": week
        },
        ReturnValues="ALL_NEW"
    )
    log.info(f'updated_art {updated_art}')


def get_trending(frame, amount):
    if frame == "hours":
        period = (datetime.utcnow() - timedelta(hours=amount)).isoformat()
    elif frame == "days":
        period = (datetime.utcnow() - timedelta(days=amount)).isoformat()

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

    arts = []
    for i in sorted_arts:
        arts.append(i['art_id'])

    set_config(arts[:250])

    return sorted_arts
