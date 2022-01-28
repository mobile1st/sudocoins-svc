import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
import json
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))
    # body = event
    collection_id = body.get('collection_id')
    art_id = body.get('art_id')

    if collection_id is None:
        try:
            art = dynamodb.Table('art').get_item(Key={'art_id': art_id})['Item']
            collection_address = art.get('open_sea_data', {}).get('asset', {}).get('asset_contract', {}).get('address')
            collection_name = art.get('open_sea_data', {}).get('asset', {}).get('collection', {}).get('name')
            c_name = ("-".join(collection_name.split())).lower()
            collection_id = collection_address + ":" + c_name
        except:
            time_now = str(datetime.utcnow().isoformat())
            return {
                'related': get_rec(20,time_now)
            }

    recent = get_recent(art_id, collection_id)

    return {
        'related': recent
    }


def get_recent(art_id, collection_id):
    # returns the artists uploaded art sorted by timestamp
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        IndexName='collection_id-event_date-index',
        Limit=20,
        FilterExpression='art_id  <> :art_value',
        ExpressionAttributeValues={':art_value': art_id},
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='art_url, art_id, list_price, preview_url, #n, tags, last_sale_price, description, collection_id, collection_data, collection_name, blockchain'
    )

    uploads = data['Items']

    art_ids = [i['art_id'] for i in uploads]

    art_list = arts.get_arts(art_ids)

    art_index = {}
    for art in art_list:
        art_index[art['art_id']] = art

    sanitized = []  # remove art that is no longer present in the art table
    for a in uploads:
        idx = art_index.get(a['art_id'])
        if not idx:
            log.info(f'Could not find art_id {a["art_id"]} in art table, hiding from user arts too')
            continue

        a['art_url'] = idx['art_url']  # this maybe a cdn url
        if idx.get('mime_type'):
            a['mime_type'] = idx.get('mime_type')
        sanitized.append(a)

    # newlist = sorted(sanitized, key=lambda k: int(k['last_sale_price']), reverse=True)

    return sanitized


def get_rec(count, timestamp):
    log.info(f"art.get_recent {count} {timestamp}")
    res = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
        ScanIndexForward=False,
        Limit=count,
        IndexName='Recent_index',
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk, mime_type, cdn_url, last_sale_price, collection_data, collection_address, open_sea_data, description, collection_id, blockchain",
        ExpressionAttributeNames={'#n': 'name'}
    )
    if not res.get('Items'):
        return None

    return res['Items']