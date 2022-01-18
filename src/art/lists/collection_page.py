import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    log.info(event)
    if 'queryStringParameters' in event:
        query_params = event.get('queryStringParameters')
        collection_id = query_params.get('collection-id')
        collection_url = None
    else:
        body = json.loads(event['body'])
        #body = event['body']
        collection_url = body.get('collection_url')
        collection_id = None
    if collection_id is None and collection_url is not None:
        data = dynamodb.Table('collections').query(
            KeyConditionExpression=Key('collection_url').eq(collection_url),
            ScanIndexForward=False,
            Limit=1,
            IndexName='collection_url-index',
            ProjectionExpression='collection_id'
        )

        collection_id = data['Items'][0]['collection_id']

    last_sale_price = get_lsp(collection_id)
    recent = get_recent(collection_id)

    return {
        'art': last_sale_price,
        'recent': recent
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_recent(collection_id):
    # returns the artists uploaded art sorted by timestamp
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        Limit=25,
        IndexName='collection_id-event_date-index',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='art_url, art_id, list_price, preview_url, #n, ast_sale_price, description, collection_id, collection_data, collection_name, blockchain, last_sale_price, collection_url, token_id,collection_item_url'
    )

    uploads = data['Items']

    while 'LastEvaluatedKey' in data and len(uploads) < 25:
        data = dynamodb.Table('art').query(
            KeyConditionExpression=Key('collection_id').eq(collection_id),
            ScanIndexForward=False,
            Limit=25,
            IndexName='collection_id-event_date-index',
            ExpressionAttributeNames={'#n': 'name'},
            ProjectionExpression='art_url, art_id, list_price, preview_url, #n, collection_address, description, collection_id, collection_data, collection_name, blockchain, last_sale_price, collection_url, token_id, collection_item_url',
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        uploads.extend(data['Items'])

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


def get_lsp(collection_id):
    # returns the artists uploaded art sorted by timestamp
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        Limit=25,
        IndexName='collection_id-last_sale_price-index',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='art_url, art_id, preview_url, #n, last_sale_price, collection_data, collection_address, open_sea_data.description, description, collection_id, blockchain, collection_url, token_id, collection_item_url'
    )

    uploads = data['Items']

    while 'LastEvaluatedKey' in data and len(uploads) < 25:
        data = dynamodb.Table('art').query(
            KeyConditionExpression=Key('collection_id').eq(collection_id),
            ScanIndexForward=False,
            Limit=25,
            IndexName='collection_id-last_sale_price-index',
            ExpressionAttributeNames={'#n': 'name'},
            ProjectionExpression='art_url, art_id, preview_url, #n, last_sale_price, collection_address, collection_data, open_sea_data.description, description, collection_id, blockchain, collection_url, token_id, collection_item_url',
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        uploads.extend(data['Items'])

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