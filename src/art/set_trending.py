import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    hour, day, week = get_trending()
    set_config(hour, day, week)

    return


def set_config(hour, day, week):
    config_table = dynamodb.Table('Config')
    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art, trending_hour=:hour, trending_day=:day, trending_week=:week",
        ExpressionAttributeValues={
            ":art": day,
            ":hour": hour,
            ":day": day,
            ":week": week
        },
        ReturnValues="ALL_NEW"
    )
    log.info(f'updated_art {updated_art}')


def get_trending():
    period = (datetime.utcnow() - timedelta(days=7)).isoformat()
    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price, event_date"
    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price, event_date",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['last_sale_price'], reverse=True)

    hour = []
    day = []
    week = []
    for i in sorted_arts:
        week.append(i['art_id'])
        if i['event_date'] > (datetime.utcnow() - timedelta(hours=1)).isoformat():
            hour.append(i['art_id'])
        if i['event_date'] > (datetime.utcnow() - timedelta(days=1)).isoformat():
            day.append(i['art_id'])

    return hour[0:250], day[0:250], week[0:250]
