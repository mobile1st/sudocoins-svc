import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    hour, half_day, day, leaders = get_trending()

    set_config(hour, half_day, day, leaders)

    return


def set_config(hour, half_day, day, artists):
    config_table = dynamodb.Table('Config')
    config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art, trending_hour=:hour, trending_half_day=:hday, trending_day=:day",
        ExpressionAttributeValues={
            ":art": hour,
            ":hour": hour,
            ":hday": half_day,
            ":day": day
        },
        ReturnValues="ALL_NEW"
    )
    config_table.update_item(
        Key={'configKey': 'Leaderboard'},
        UpdateExpression="set creators=:create",
        ExpressionAttributeValues={
            ":create": artists
        }
    )
    log.info("configs updated")


def get_trending():
    period = (datetime.utcnow() - timedelta(days=1)).isoformat()
    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data"
    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['last_sale_price'], reverse=True)

    hour = []
    half_day = []
    day = []
    artists = {}

    for i in sorted_arts:
        day.append(i['art_id'])
        if i['event_date'] > (datetime.utcnow() - timedelta(hours=1)).isoformat():
            hour.append(i['art_id'])
        if i['event_date'] > (datetime.utcnow() - timedelta(hours=12)).isoformat():
            half_day.append(i['art_id'])

        if i['creator'] in artists:
            artists[i['creator']]['score'] += i.get('last_sale_price')
        else:
            artists[i['creator']] = {}
            artists[i['creator']]['score'] = i.get('last_sale_price')
            artists[i['creator']]['avatar'] = i.get('preview_url')
            artists[i['creator']]['collection_name'] = i.get('collection_data', {}).get('name')
            artists[i['creator']]['data'] = {}
            artists[i['creator']]['data']['address'] = i.get('creator')
            artists[i['creator']]['data']['profile_img_url'] = i.get('preview_url')
            artists[i['creator']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

            if 'creator' in i['open_sea_data'] and 'user' in i['open_sea_data']['creator'] and 'username' in i['open_sea_data']['creator']['user'] and i['open_sea_data']['creator']['user']['username'] is not None :
                artists[i['creator']]['name'] = i['open_sea_data']['creator']['user']
            elif 'collection_data' in i and 'name' in i['collection_data'] and i['collection_data']['name'] is not None:
                artists[i['creator']]['name'] = i['collection_data']['name']
            else:
                artists[i['creator']]['name'] = i.get('creator')



    leaders = sorted(artists.values(), key=lambda x: x['score'], reverse=True)[:250]

    return hour[0:250], half_day[0:250], day[0:250], leaders
