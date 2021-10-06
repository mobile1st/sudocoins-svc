import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime
from art.art import Art
from util import sudocoins_logger
from art.ledger import Ledger
from decimal import Decimal, getcontext

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")
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
    if url.find('klaytn') > 0:
        sub1 = url.find('assets/')
        start = sub1 + 6
        rest = url[start:]
        variables = rest.split('/')
        contract_id = variables[0]
        token_id = variables[1]

        return contract_id, token_id
    elif url.find('matic') > 0:
        sub1 = url.find('matic/')
        start = sub1 + 6
        rest = url[start:]
        variables = rest.split('/')
        contract_id = variables[0]
        token_id = variables[1]

        return contract_id, token_id
    else:
        sub1 = url.find('assets/')
        start = sub1 + 7
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
            "asset": art_object.get('asset'),
            "token_metadata": art_object.get('asset', {}).get('token_metadata')
        }
    else:
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
            "asset": art_object.get('asset'),
            "token_metadata": art_object.get('asset', {}).get('token_metadata')
        }

    preview_url, art_url = get_urls(open_sea)
    if art_url == "" and preview_url is None:
        log.info("missing art_url and preview_url")
        return
    buy_url = open_sea['permalink'] if open_sea.get('permalink') else art_object.get('open_sea_url')
    eth_sale_price = eth_price(art_object)

    get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price)

    return


def get_urls(open_sea):
    if open_sea.get('animation_original_url'):
        return open_sea["image_preview_url"], open_sea["animation_original_url"]

    if open_sea.get('image_original_url'):
        return open_sea["image_preview_url"], open_sea['image_original_url']

    return open_sea["image_preview_url"], open_sea['image_url']


def get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price):
    contract_token_id = str(contract_id) + "#" + str(token_id)
    art_id = art.get_id(contract_token_id)
    if art_id:
        return update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price)

    return art.auto_add(contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price)


def update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price):
    dynamodb.Table('art').update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET art_url=:art, buy_url=:buy, preview_url=:pre, open_sea_data=:open,"
                         "last_sale_price=:lsp, event_date=:ed, #n=:na, collection_address=:ca, collection_data=:cd,"
                         "collection_name=:cn, #o=:ow",
        ExpressionAttributeValues={
            ':art': art_url,
            ':buy': buy_url,
            ':pre': preview_url,
            ':open': open_sea,
            ':lsp': eth_sale_price,
            ":ed": art_object.get('created_date'),
            ":na": open_sea.get('name'),
            ":ca": art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown"),
            ":cd": {
                "name": art_object.get('asset', {}).get('collection', {}).get('name'),
                "image_url": art_object.get('asset', {}).get('collection', {}).get('image_url'),
                "description": art_object.get('asset', {}).get('collection', {}).get('description', "")
            },
            ":cn": art_object.get('asset', {}).get('collection', {}).get('name'),
            ":ow": art_object.get('asset', {}).get("owner", {}).get('address', "")
        },
        ExpressionAttributeNames={'#n': 'name', '#o': 'owner'}
    )

    log.info("art record updated")
    log.info(f"art_id: {art_id}")

    return


def eth_price(art_object):
    getcontext().prec = 18
    symbol = art_object.get("payment_token", {})
    if symbol is None:
        symbol = "ETH"
    else:
        symbol = symbol.get("symbol", "ETH")

    log.info(f'symbol: {symbol}')
    if symbol != 'ETH':
        total_price = Decimal(art_object.get("sale_price", 0))
        eth_price = Decimal(art_object.get("payment_token", {}).get("eth_price", "1"))
        eth_sale_price = int(total_price * eth_price)

    else:
        eth_sale_price = int(art_object.get("sale_price", 0))

    return eth_sale_price



