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

    if len(hour) is 0:
        trending = day
    else:
        trending = hour

    config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art, trending_hour=:hour, trending_half_day=:hday, trending_day=:day",
        ExpressionAttributeValues={
            ":art": trending,
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
        ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address"
    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address",
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['last_sale_price'], reverse=True)

    hour = []
    half_day = []
    day = []
    artists = {}

    for i in sorted_arts:
        try:
            day.append(i['art_id'])
            if i['event_date'] > (datetime.utcnow() - timedelta(hours=1)).isoformat():
                hour.append(i['art_id'])
            if i['event_date'] > (datetime.utcnow() - timedelta(hours=12)).isoformat():
                half_day.append(i['art_id'])

            if i['collection_data']['name'] in artists:
                artists[i['collection_data']['name']]['score'] += i.get('last_sale_price')
            else:
                artists[i['collection_data']['name']] = {}
                artists[i['collection_data']['name']]['score'] = i.get('last_sale_price')
                artists[i['collection_data']['name']]['avatar'] = i.get('preview_url')

                artists[i['collection_data']['name']]['data'] = {}
                artists[i['collection_data']['name']]['data']['address'] = i.get('collection_address')
                artists[i['collection_data']['name']]['data']['profile_img_url'] = i.get('preview_url')
                artists[i['collection_data']['name']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                artists[i['collection_data']['name']]['art1'] = i.get('art_id')
                artists[i['collection_data']['name']]['name'] = i.get('collection_data', {}).get('name', i['creator'])
                artists[i['collection_data']['name']]['collection_address'] = i.get('collection_address')


        except Exception as e:
            log.info(e)
            continue


    leaders = sorted(artists.values(), key=lambda x: x['score'], reverse=True)[:250]

    return hour[0:250], half_day[0:250], day[0:250], leaders
