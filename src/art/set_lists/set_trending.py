import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    arts_hour, arts_half_day, arts_day, buyers_day, buyers_half, buyers_hour = get_trending()

    set_config(arts_hour, arts_half_day, arts_day, buyers_day,
               buyers_half, buyers_hour)

    return


def set_config(arts_hour, arts_half_day, arts_day, buyers_day,
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

            if i['event_date'] > (datetime.fromisoformat(created) - timedelta(days=1)).isoformat():
                half_day.append(i['art_id'])

                if i['owner'] in owners3:
                    owners3[i['owner']]['score'] += i.get('last_sale_price')
                else:
                    owners3[i['owner']] = {}
                    owners3[i['owner']]['score'] = i.get('last_sale_price')
                    owners3[i['owner']]['preview_url'] = i.get('preview_url')
                    owners3[i['owner']]['owner_address'] = i.get('owner')

            day.append(i['art_id'])

            if i['owner'] in owners:
                owners[i['owner']]['score'] += i.get('last_sale_price')
            else:
                owners[i['owner']] = {}
                owners[i['owner']]['score'] = i.get('last_sale_price')
                owners[i['owner']]['preview_url'] = i.get('preview_url')
                owners[i['owner']]['owner_address'] = i.get('owner')



        except Exception as e:
            log.info(e)
            log.info(i['art_id'])
            continue

    buyers_day = sorted(owners.values(), key=lambda x: x['score'], reverse=True)
    buyers_hour = sorted(owners2.values(), key=lambda x: x['score'], reverse=True)
    buyers_half = sorted(owners3.values(), key=lambda x: x['score'], reverse=True)

    return hour[0:250], half_day[0:250], day[0:250], buyers_day[1:150], buyers_half[1:150], buyers_hour[1:150]
