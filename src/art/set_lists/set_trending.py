import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    arts_hour, arts_half_day, arts_day, collections_day, collections_hour, collections_half, buyers_day, buyers_half, buyers_hour = get_trending()

    set_config(arts_hour, arts_half_day, arts_day, collections_day, collections_hour, collections_half, buyers_day,
               buyers_half, buyers_hour)

    return


def set_config(arts_hour, arts_half_day, arts_day, collections_day, collections_hour, collections_half, buyers_day,
               buyers_half, buyers_hour):
    config_table = dynamodb.Table('Config')

    if len(arts_hour) == 0:
        trending_arts = arts_day
    else:
        trending_arts = arts_hour

    config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art, trending_hour=:hour, trending_half_day=:hday, trending_day=:day",
        ExpressionAttributeValues={
            ":art": trending_arts,
            ":hour": arts_hour,
            ":hday": arts_half_day,
            ":day": arts_day
        },
        ReturnValues="ALL_NEW"
    )
    config_table.update_item(
        Key={'configKey': 'Leaderboard'},
        UpdateExpression="set collections=:create, collections_hour=:create2, collections_half=:create3",
        ExpressionAttributeValues={
            ":create": collections_day,
            ":create2": collections_hour,
            ":create3": collections_half
        }
    )
    config_table.update_item(
        Key={'configKey': 'TopBuyers'},
        UpdateExpression="set buyers_day=:buy, buyers_hour=:buy2, buyers_half=:buy3",
        ExpressionAttributeValues={
            ":buy": buyers_day,
            ":buy2": buyers_hour,
            ":buy3": buyers_half
        }
    )

    log.info("configs updated")


def get_trending():
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

    period = (datetime.fromisoformat(created) - timedelta(days=7)).isoformat()

    record = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
        IndexName='top-sales',
        ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address, blockchain, #o, collection_id, collection_name",
        ExpressionAttributeNames={'#o': 'owner'}
    )
    data = record['Items']
    while 'LastEvaluatedKey' in record:
        record = dynamodb.Table('art').query(
            KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").gt(period),
            IndexName='top-sales',
            ProjectionExpression="art_id, last_sale_price, event_date, creator, preview_url, open_sea_data, collection_data, collection_address, blockchain, #o, collection_id, collection_name",
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

                if i['collection_id'] in artists2:
                    artists2[i['collection_id']]['score'] += i.get('last_sale_price')
                else:
                    artists2[i['collection_id']] = {}
                    artists2[i['collection_id']]['score'] = i.get('last_sale_price')
                    artists2[i['collection_id']]['avatar'] = i.get('preview_url')
                    artists2[i['collection_id']]['collection_id'] = i.get('collection_id')

                    artists2[i['collection_id']]['data'] = {}
                    artists2[i['collection_id']]['data']['address'] = i.get('collection_address')
                    artists2[i['collection_id']]['data']['profile_img_url'] = i.get('preview_url')
                    artists2[i['collection_id']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                    artists2[i['collection_id']]['art1'] = i.get('art_id')
                    artists2[i['collection_id']]['name'] = i.get('collection_data', {}).get('name',
                                                                                            i['collection_name'])
                    artists2[i['collection_id']]['collection_address'] = i.get('collection_address')
                    if artists2[i['collection_id']]['collection_address'] == 'unknown':
                        artists2[i['collection_id']]['collection_address'] = \
                            i['open_sea_data']['asset']['asset_contract']['address']

            if i['event_date'] > (datetime.fromisoformat(created) - timedelta(days=1)).isoformat():
                half_day.append(i['art_id'])

                if i['owner'] in owners3:
                    owners3[i['owner']]['score'] += i.get('last_sale_price')
                else:
                    owners3[i['owner']] = {}
                    owners3[i['owner']]['score'] = i.get('last_sale_price')
                    owners3[i['owner']]['preview_url'] = i.get('preview_url')
                    owners3[i['owner']]['owner_address'] = i.get('owner')

                if i['collection_id'] in artists3:
                    artists3[i['collection_id']]['score'] += i.get('last_sale_price')
                else:
                    artists3[i['collection_id']] = {}
                    artists3[i['collection_id']]['score'] = i.get('last_sale_price')
                    artists3[i['collection_id']]['avatar'] = i.get('preview_url')
                    artists3[i['collection_id']]['collection_id'] = i.get('collection_id')

                    artists3[i['collection_id']]['data'] = {}
                    artists3[i['collection_id']]['data']['address'] = i.get('collection_address')
                    artists3[i['collection_id']]['data']['profile_img_url'] = i.get('preview_url')
                    artists3[i['collection_id']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                    artists3[i['collection_id']]['art1'] = i.get('art_id')
                    artists3[i['collection_id']]['name'] = i.get('collection_data', {}).get('name',
                                                                                            i['collection_name'])
                    artists3[i['collection_id']]['collection_address'] = i.get('collection_address')
                    if artists3[i['collection_id']]['collection_address'] == 'unknown':
                        artists3[i['collection_id']]['collection_address'] = \
                            i['open_sea_data']['asset']['asset_contract']['address']

            day.append(i['art_id'])

            if i['owner'] in owners:
                owners[i['owner']]['score'] += i.get('last_sale_price')
            else:
                owners[i['owner']] = {}
                owners[i['owner']]['score'] = i.get('last_sale_price')
                owners[i['owner']]['preview_url'] = i.get('preview_url')
                owners[i['owner']]['owner_address'] = i.get('owner')

            if i['collection_id'] in artists:
                artists[i['collection_id']]['score'] += i.get('last_sale_price')
            else:
                artists[i['collection_id']] = {}
                artists[i['collection_id']]['score'] = i.get('last_sale_price')
                artists[i['collection_id']]['avatar'] = i.get('preview_url')
                artists[i['collection_id']]['collection_id'] = i.get('collection_id')

                artists[i['collection_id']]['data'] = {}
                artists[i['collection_id']]['data']['address'] = i.get('collection_address')
                artists[i['collection_id']]['data']['profile_img_url'] = i.get('preview_url')
                artists[i['collection_id']]['data']['user'] = i.get('open_sea_data', {}).get('creator')

                artists[i['collection_id']]['art1'] = i.get('art_id')
                artists[i['collection_id']]['name'] = i.get('collection_data', {}).get('name', i['collection_name'])
                artists[i['collection_id']]['collection_address'] = i.get('collection_address')
                if artists[i['collection_id']]['collection_address'] == 'unknown':
                    artists[i['collection_id']]['collection_address'] = i['open_sea_data']['asset']['asset_contract'][
                        'address']

        except Exception as e:
            log.info(e)
            log.info(i['art_id'])
            continue

    collections_day = sorted(artists.values(), key=lambda x: x['score'], reverse=True)
    collections_hour = sorted(artists2.values(), key=lambda x: x['score'], reverse=True)
    collections_half = sorted(artists3.values(), key=lambda x: x['score'], reverse=True)

    buyers_day = sorted(owners.values(), key=lambda x: x['score'], reverse=True)
    buyers_hour = sorted(owners2.values(), key=lambda x: x['score'], reverse=True)
    buyers_half = sorted(owners3.values(), key=lambda x: x['score'], reverse=True)

    return hour[0:250], half_day[0:250], day[0:250], collections_day[0:75], collections_hour[0:75], collections_half[
                                                                                                      0:75], buyers_day[
                                                                                                              1:150], buyers_half[
                                                                                                                      1:150], buyers_hour[
                                                                                                                              1:150]