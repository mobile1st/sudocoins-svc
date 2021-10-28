import boto3
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger
from art.art import Art
import http.client
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)



def lambda_handler(event, context):
    # returns the art shared by the user
    set_log_context(event)
    sub = event['pathParameters']['userId']

    return {
        'art': get_owned(sub),
        "owned": get_owned(sub),
        "created": get_created(sub)
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_owned(sub):
    # returns the user's uploaded art sorted by timestamp

    uploads = dynamodb.Table('art').query(
        KeyConditionExpression=Key('owner').eq(sub),
        ScanIndexForward=False,
        IndexName='owner-recent_sk-index',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='art_url, art_id, preview_url, #n, last_sale_price, list_price, description, collection_id, collection_name'
    )['Items']

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
            a['last_sale_price'] = idx.get('last_sale_price')
        sanitized.append(a)

    return sanitized


def get_created(sub):
    # returns the user's uploaded art sorted by timestamp

    uploads = dynamodb.Table('art').query(
        KeyConditionExpression=Key('creator').eq(sub),
        ScanIndexForward=False,
        IndexName='creator-recent_sk-index',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='art_url, art_id, preview_url, #n, last_sale_price, list_price, description, collection_id, collection_name'
    )['Items']

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
            a['last_sale_price'] = idx.get('last_sale_price')
        sanitized.append(a)

    return sanitized


def get_metamask_arts(public_address):
    path = "https://api.opensea.io/api/v1/assets?owner=" + public_address + "&limit=50&offset=" + "0"
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)['assets']

    arts = []
    arts = arts + open_sea_response

    count = 50
    while len(open_sea_response) >= 50:
        path = "https://api.opensea.io/api/v1/assets?owner=" + public_address + "&limit=50&offset=" + str(count)
        log.info(f'path: {path}')
        conn = http.client.HTTPSConnection("api.opensea.io")
        conn.request("GET", path)
        response = conn.getresponse()
        response2 = response.read().decode('utf-8')
        open_sea_response = json.loads(response2)['assets']
        count += 50
        arts = arts + open_sea_response

    art_objects = []
    for i in arts:
        contract = i.get("asset_contract", {}).get("address", "")
        token = i.get("id", "")
        contract_token = contract + "#" + str(token)

        msg = {
            "art_url": i.get("image_url", ""),
            "preview_url": i.get("image_preview_url", ""),
            "name": i.get("name", ""),
            "description": i.get("description", ""),
            "collection_address": i.get("asset_contract", {}).get("address", ""),
            "collection_name": i.get("collection", {}).get("name", ""),
            "contractId#tokenId": contract_token
        }

        if i['last_sale'] is not None:
            msg['last_sale_price'] = i.get('last_sale', {}).get('total_price')

        if i['sell_orders'] is not None and len(i['sell_orders']) > 0:
            msg['list_price'] = i.get('sell_orders')[0]['current_price']

        art_objects.append(msg)


    return art_objects