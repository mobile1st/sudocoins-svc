import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    query_params = event['queryStringParameters']
    owner_address = query_params['owner']

    last_sale_price = high_price(owner_address)
    recent = recently_bought(owner_address)
    sold = recently_sold(owner_address)

    return {
        'last_sale_price': last_sale_price,
        'recent': recent,
        'recently_sold': sold
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def recently_bought(owner_address):
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('owner').eq(owner_address),
        ScanIndexForward=False,
        IndexName='owner-recent_sk-index',
        ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
        ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, open_sea_data.description, description, collection_id, #o, collection_data'
    )

    uploads = data['Items']

    while 'LastEvaluatedKey' in data and len(uploads) < 250:
        data = dynamodb.Table('art').query(
            KeyConditionExpression=Key('owner').eq(owner_address),
            ScanIndexForward=False,
            IndexName='owner-recent_sk-index',
            ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
            ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, collection_address, open_sea_data.description, description, collection_id, #o, collection_data',
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

    return sanitized


def high_price(owner_address):
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('owner').eq(owner_address),
        ScanIndexForward=False,
        IndexName='owner-last_sale_price-index',
        ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
        ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, collection_data, collection_address, open_sea_data.description, description, collection_id, #o'
    )

    uploads = data['Items']

    while 'LastEvaluatedKey' in data and len(uploads) < 250:
        data = dynamodb.Table('art').query(
            KeyConditionExpression=Key('owner').eq(owner_address),
            ScanIndexForward=False,
            IndexName='owner-last_sale_price-index',
            ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
            ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, collection_address, collection_data, open_sea_data.description, description, collection_id, #o',
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

    return sanitized


def recently_sold(owner_address):
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('owner').eq(owner_address),
        ScanIndexForward=False,
        IndexName='seller-recent_sk-index',
        ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
        ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, collection_data, collection_address, open_sea_data.description, description, collection_id, #o'
    )

    uploads = data['Items']

    while 'LastEvaluatedKey' in data and len(uploads) < 250:
        data = dynamodb.Table('art').query(
            KeyConditionExpression=Key('owner').eq(owner_address),
            ScanIndexForward=False,
            IndexName='seller-recent_sk-index',
            ExpressionAttributeNames={'#n': 'name', '#o': 'owner'},
            ProjectionExpression='click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, collection_address, collection_data, open_sea_data.description, description, collection_id, #o',
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

    return sanitized