import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    hour, half_day, day, leaders, leaders_hour, leaders_half, buyers, buyers_half, buyers_hour = get_trending()

    set_config(hour, half_day, day, leaders, leaders_hour, leaders_half, buyers, buyers_half, buyers_hour)

    return


def set_config(hour, half_day, day, artists, artists_hour, artists_half, buyers, buyers_half, buyers_hour):
    config_table = dynamodb.Table('Config')

    if len(hour) == 0:
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
        UpdateExpression="set creators=:create, creators_hour=:create2, creators_half=:create3",
        ExpressionAttributeValues={
            ":create": artists,
            ":create2": artists_hour,
            ":create3": artists_half
        }
    )
    config_table.update_item(
        Key={'configKey': 'Leaderboard'},
        UpdateExpression="set buyers=:buy, buyers_hour=:buy2, buyers_half=:buy3",
        ExpressionAttributeValues={
            ":buy": buyers,
            ":buy2": buyers_hour,
            ":buy3": buyers_half
        }
    )


    log.info("configs updated")


def get_trending():
    period = (datetime.utcnow() - timedelta(days=1)).isoformat()
    time_now = datetime.utcnow().isoformat()
    log.info(time_now)

    created = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").lt(time_now),
        ScanIndexForward=False,
        Limit=1,
        IndexName='sort_idx-event_date-index',
        ProjectionExpression="event_date"
    )['Items'][0]['event_date']
    log.info(created)

    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address, blockchain, #o, collection_id",
        ExpressionAttributeNames={'#o': 'owner'}
    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address, blockchain, #o, collection_id",
            ExpressionAttributeNames={'#o': 'owner'},
            ExclusiveStartKey=record['LastEvaluatedKey']
        )
        data.extend(record['Items'])

    sorted_arts = sorted(data, key=lambda item: item['last_sale_price'], reverse=True)

    hour = []
    half_day = []
    day = []

    artists = {}
    artists2 = {}
    artists3 = {}

    owners = {}
    owners2 = {}
    owners3 = {}

    for i in sorted_arts:
        try:
            if i['blockchain'] != 'Ethereum':
                continue

            if i['event_date'] > (datetime.fromisoformat(created) - timedelta(hours=1)).isoformat():
                hour.append(i['art_id'])

                if i['owner'] in owners2:
                    owners2[i['owner']]['score'] += i.get('last_sale_price')
                else:
                    owners2[i['owner']] = {}
                    owners2[i['owner']]['score'] = i.get('last_sale_price')
                    owners2[i['owner']]['preview_url'] = i.get('preview_url')
                    owners2[i['owner']]['owner_address'] = i.get('owner')

                if i['collection_data']['name'] in artists2:
                    artists2[i['collection_data']['name']]['score'] += i.get('last_sale_price')
                else:
                    artists2[i['collection_data']['name']] = {}
                    artists2[i['collection_data']['name']]['score'] = i.get('last_sale_price')
                    artists2[i['collection_data']['name']]['avatar'] = i.get('preview_url')

                    artists2[i['collection_data']['name']]['data'] = {}
                    artists2[i['collection_data']['name']]['data']['address'] = i.get('collection_address')
                    artists2[i['collection_data']['name']]['data']['profile_img_url'] = i.get('preview_url')
                    artists2[i['collection_data']['name']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                    artists2[i['collection_data']['name']]['art1'] = i.get('art_id')
                    artists2[i['collection_data']['name']]['name'] = i.get('collection_data', {}).get('name',
                                                                                                      i['creator'])
                    artists2[i['collection_data']['name']]['collection_address'] = i.get('collection_address')
                    if artists2[i['collection_data']['name']]['collection_address'] == 'unknown':
                        artists2[i['collection_data']['name']]['collection_address'] = \
                            i['open_sea_data']['asset']['asset_contract']['address']

            if i['event_date'] > (datetime.fromisoformat(created) - timedelta(hours=12)).isoformat():
                half_day.append(i['art_id'])

                if i['owner'] in owners3:
                    owners3[i['owner']]['score'] += i.get('last_sale_price')
                else:
                    owners3[i['owner']] = {}
                    owners3[i['owner']]['score'] = i.get('last_sale_price')
                    owners3[i['owner']]['preview_url'] = i.get('preview_url')
                    owners3[i['owner']]['owner_address'] = i.get('owner')

                if i['collection_data']['name'] in artists3:
                    artists3[i['collection_data']['name']]['score'] += i.get('last_sale_price')
                else:
                    artists3[i['collection_data']['name']] = {}
                    artists3[i['collection_data']['name']]['score'] = i.get('last_sale_price')
                    artists3[i['collection_data']['name']]['avatar'] = i.get('preview_url')

                    artists3[i['collection_data']['name']]['data'] = {}
                    artists3[i['collection_data']['name']]['data']['address'] = i.get('collection_address')
                    artists3[i['collection_data']['name']]['data']['profile_img_url'] = i.get('preview_url')
                    artists3[i['collection_data']['name']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                    artists3[i['collection_data']['name']]['art1'] = i.get('art_id')
                    artists3[i['collection_data']['name']]['name'] = i.get('collection_data', {}).get('name',
                                                                                                      i['creator'])
                    artists3[i['collection_data']['name']]['collection_address'] = i.get('collection_address')
                    if artists3[i['collection_data']['name']]['collection_address'] == 'unknown':
                        artists3[i['collection_data']['name']]['collection_address'] = \
                            i['open_sea_data']['asset']['asset_contract']['address']

            day.append(i['art_id'])

            if i['owner'] in owners:
                owners[i['owner']]['score'] += i.get('last_sale_price')
            else:
                owners[i['owner']] = {}
                owners[i['owner']]['score'] = i.get('last_sale_price')
                owners[i['owner']]['preview_url'] = i.get('preview_url')
                owners[i['owner']]['owner_address'] = i.get('owner')

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
                if artists[i['collection_data']['name']]['collection_address'] == 'unknown':
                    artists[i['collection_data']['name']]['collection_address'] = \
                    i['open_sea_data']['asset']['asset_contract']['address']



        except Exception as e:
            log.info(e)
            log.info(i['art_id'])
            continue

    leaders = sorted(artists.values(), key=lambda x: x['score'], reverse=True)
    leaders_hour = sorted(artists2.values(), key=lambda x: x['score'], reverse=True)
    leaders_half = sorted(artists3.values(), key=lambda x: x['score'], reverse=True)

    buyers = sorted(owners.values(), key=lambda x: x['score'], reverse=True)
    buyers_hour = sorted(owners2.values(), key=lambda x: x['score'], reverse=True)
    buyers_half = sorted(owners3.values(), key=lambda x: x['score'], reverse=True)

    return hour[0:250], half_day[0:250], day[0:250], leaders[0:175], leaders_hour[0:175], leaders_half[0:175], buyers[0:125], buyers_half[0:125], buyers_hour[0:125]
