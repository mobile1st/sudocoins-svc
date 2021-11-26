import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))
    collection_id = body['collection_id']
    art_id = body['art_id']

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
        ProjectionExpression='art_url, art_id, list_price, preview_url, #n, tags, last_sale_price, description, collection_id, collection_data, collection_name'
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

    #newlist = sorted(sanitized, key=lambda k: int(k['last_sale_price']), reverse=True)

    return sanitized