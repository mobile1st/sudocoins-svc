import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime
from art.art import Art
from util import sudocoins_logger
from art.ledger import Ledger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
ledger = Ledger(dynamodb)
art = Art(dynamodb)


def lambda_handler(event, context):
    art_object = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {art_object}')
    if art_object['open_sea_url'] is None:
        log.info("open sea url doesn't exist")
        return
    try:
        add(art_object)
        log.info('status: success')
    except Exception as e:
        log.info('status: failure')
        log.info(e)

    return


def parse_url(url):
    if url.find('matic') == -1:
        sub1 = url.find('assets/')
        start = sub1 + 7
        rest = url[start:]
        variables = rest.split('/')
        log.debug(f'variables {variables}')
        contract_id = variables[0]
        token_id = variables[1]

        return contract_id, token_id
    else:
        sub1 = url.find('matic/')
        start = sub1 + 6
        rest = url[start:]
        variables = rest.split('/')
        contract_id = variables[0]
        token_id = variables[1]

        return contract_id, token_id



def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def call_open_sea(contract_id, token_id):
    open_sea_url_pattern = "/api/v1/asset/{0}/{1}"
    path = open_sea_url_pattern.format(contract_id, token_id)
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)
    log.info(f'open_sea_response: {open_sea_response}')
    return open_sea_response


def add(art_object):

    if art_object['blockchain'] == "Ethereum":
        contract_id, token_id = parse_url(art_object['open_sea_url'])
        open_sea_response = call_open_sea(contract_id, token_id)
        open_sea = {
            'redirect': art_object['open_sea_url'],
            'name': open_sea_response.get('name', ""),
            'description': open_sea_response['description'],
            "image_url": open_sea_response['image_url'],
            "image_preview_url": open_sea_response['image_preview_url'],
            "image_thumbnail_url": open_sea_response['image_thumbnail_url'],
            "image_original_url": open_sea_response['image_original_url'],
            "animation_url": open_sea_response['animation_url'],
            "animation_original_url": open_sea_response['animation_original_url'],
            "creator": open_sea_response['creator'],
            "permalink": open_sea_response['permalink'],
            "collection": art_object.get('collection'),
            "token_metadata": art_object.get('asset', {}).get('token_metadata')
        }
    elif art_object['blockchain'] == "Polygon":
        contract_id, token_id = parse_url(art_object['open_sea_url'])
        open_sea = {
            'redirect': art_object.get('open_sea_url'),
            'name': art_object.get('asset', {}).get('name', ""),
            'description': art_object.get('asset', {}).get('description'),
            "image_url": art_object.get('asset', {}).get('image_url'),
            "image_preview_url": art_object.get('asset', {}).get('image_preview_url'),
            "image_thumbnail_url": art_object.get('asset', {}).get('image_thumbnail_url'),
            "image_original_url": art_object.get('asset', {}).get('image_original_url'),
            "animation_url": art_object.get('asset', {}).get('animation_url'),
            "animation_original_url": art_object.get('asset', {}).get('animation_original_url'),
            "creator": art_object.get('asset', {}).get('asset_contract'),
            "permalink": art_object.get('open_sea_url'),
            "collection": art_object.get('collection'),
            "token_metadata": art_object.get('asset', {}).get('token_metadata')
        }

    preview_url, art_url = get_urls(open_sea)

    buy_url = open_sea['permalink'] if open_sea.get('permalink') else art_object.get('open_sea_url')
    user_id = "ingest"
    tags = []

    get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, user_id, tags, art_object)

    try:
        if art_object['blockchain'] == "Ethereum":
            dynamodb.Table('creators').put_item(
                Item={
                    'address': open_sea_response['creator']['address'],
                    'open_sea_data': open_sea_response['creator'],
                    'timestamp': str(datetime.utcnow().isoformat()),
                    'last_update': str(datetime.utcnow().isoformat())
                },
                ConditionExpression='attribute_not_exists(address)'
            )
        else:
            dynamodb.Table('creators').put_item(
                Item={
                    'address': open_sea['creator']['address'],
                    'open_sea_data': open_sea['creator'],
                    'timestamp': str(datetime.utcnow().isoformat()),
                    'last_update': str(datetime.utcnow().isoformat())
                },
                ConditionExpression='attribute_not_exists(address)'
            )

    except Exception as e:
        log.info(e)

    return


def get_urls(open_sea):
    if open_sea.get('animation_original_url'):
        return open_sea["image_preview_url"],  open_sea["animation_original_url"]

    if open_sea.get('image_original_url'):
        return open_sea["image_preview_url"], open_sea['image_original_url']

    return open_sea["image_preview_url"], open_sea['image_url']


def get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, user_id, tags, art_object):
    contract_token_id = str(contract_id) + "#" + str(token_id)
    art_id = art.get_id(contract_token_id)
    if art_id:
        return update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object)

    return art.auto_add(contract_token_id, art_url, preview_url, buy_url, open_sea, user_id, tags, art_object)['art_id']


def update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object):
    dynamodb.Table('art').update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET art_url=:art, buy_url=:buy, preview_url=:pre, open_sea_data=:open,"
                         "last_sale_price=:lsp, event_date=:ed, #n=:na",
        ExpressionAttributeValues={
            ':art': art_url,
            ':buy': buy_url,
            ':pre': preview_url,
            ':open': open_sea,
            ':lsp': int(art_object.get("sale_price_token")),
            ":ed": art_object.get('created_date'),
            ":na": open_sea.get('name')
        },
        ExpressionAttributeNames={'#n': 'name'}
    )

    log.info("art record updated")

    return
