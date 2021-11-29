import boto3
from util import sudocoins_logger
import http.client
import json
from art.art import Art
from util import sudocoins_logger
from art.ledger import Ledger
from decimal import Decimal, getcontext
import pymysql
from datetime import datetime
import uuid

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")
ledger = Ledger(dynamodb)
art = Art(dynamodb)
sns = boto3.client("sns")


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

    buy_url = open_sea['permalink'] if open_sea.get('permalink') else art_object.get('open_sea_url')
    preview_url, art_url = get_urls(open_sea)
    if art_url == "" and preview_url is None:
        log.info("missing art_url and preview_url")
        return

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
        return update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price, contract_token_id)

    return auto_add(contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price)


def update_art(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price, contract_token_id):

    collection_address = art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown")
    collection_name = art_object.get('asset', {}).get('collection', {}).get('name')
    c_name = ("-".join(collection_name.split())).lower()
    collection_id = collection_address + ":" + c_name

    try:
        rds_host = "rds-proxy.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
        name = "admin"
        password = "RHV2CiqtjiZpsM11"
        db_name = "nft_events"

        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
        with conn.cursor() as cur:
            art_id = art_id
            price = eth_sale_price
            collection_id = collection_id
            collection_name = art_object.get('asset', {}).get('collection', {}).get('name')
            contract_token = contract_token_id
            event_date = art_object.get('created_date')
            time = str(datetime.utcnow().isoformat())
            blockchain = art_object.get('blockchain')
            buyer = art_object.get("owner")
            seller = art_object.get("seller")
            event_type = 'successful'
            row_values = (
            art_id, price, collection_id, collection_name, contract_token, event_date, time, blockchain, event_type, buyer, seller, collection_address)
            cur.execute(
                'INSERT INTO `nft_events`.`open_sea_events` (`art_id`, `price`, `collection_id`, `collection_name`,`contract_token_id`, `event_date`, `created_date`, `blockchain`, `event_type`,`buyer`,`seller`,`collection_address`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)',
                row_values)
            conn.commit()
            log.info("rds updated")

    except Exception as e:
        log.info(e)


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


def auto_add(contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price):
    dynamodb = boto3.resource('dynamodb')
    time_now = str(datetime.utcnow().isoformat())
    art_id = str(uuid.uuid1())
    art_record = {
        'art_id': art_id,
        "name": open_sea['name'],
        'buy_url': buy_url,
        'contractId#tokenId': contract_token_id,
        'preview_url': preview_url,
        'art_url': art_url,
        "open_sea_data": open_sea,
        "timestamp": time_now,
        "recent_sk": time_now + "#" + art_id,
        "click_count": 0,
        "first_user": "ingest",
        "sort_idx": 'true',
        "process_status": "STREAM_TO_S3",
        "event_date": art_object.get('created_date'),
        "event_type": art_object.get('event_type'),
        "blockchain": art_object.get('blockchain'),
        "last_sale_price": eth_sale_price,
        "collection_address": art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown"),
        "collection_data": {
            "name": art_object.get('asset', {}).get('collection', {}).get('name'),
            "image_url": art_object.get('asset', {}).get('collection', {}).get('image_url'),
            "description": art_object.get('asset', {}).get('collection', {}).get('description', ""),
            "discord": art_object.get('asset', {}).get('collection', {}).get('discord_url', ""),
            "twitter": art_object.get('asset', {}).get('collection', {}).get('twitter_username', ""),
            "instagram": art_object.get('asset', {}).get('collection', {}).get('instagram_username', ""),
            "website": art_object.get('asset', {}).get('collection', {}).get('external_url', "")
        },
        "process_to_google_search": "TO_BE_INDEXED",
        "collection_name": art_object.get('asset', {}).get('collection', {}).get('name'),
        "owner": art_object.get("owner", "unknown"),
        "seller": art_object.get("seller", "unknown")
    }

    if art_record['collection_name'] is not None and art_record['collection_address'] is not None:
        c_name = ("-".join(art_record['collection_name'].split())).lower()
        art_record['collection_id'] = art_record['collection_address'] + ":" + c_name
    else:
        art_record['collection_id'] = art_record['collection_address']

    if art_record['preview_url'] is None:
        art_record['preview_url'] = art_record['art_url']

    if 'name' in art_record and art_record['name'] is None:
        name = art_record.get('collection_data', {}).get('name', "")
        number = art_record.get("contractId#tokenId", "")
        number = number.split('#')[1]
        art_record['name'] = name + " #" + str(number)

    try:
        rds_host = "rds-proxy.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
        name = "admin"
        password = "RHV2CiqtjiZpsM11"
        db_name = "nft_events"

        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
        with conn.cursor() as cur:
            art_id = art_record['art_id']
            price = art_record['last_sale_price']
            collection_id = art_record['collection_id']
            collection_name = art_record['collection_name']
            contract_token = art_record['contractId#tokenId']
            event_date = art_record['event_date']
            time = time_now
            blockchain = art_record['blockchain']
            event_type = 'successful'
            buyer = art_record['owner']
            seller = art_record['seller']
            contract_address = art_record['collection_address']
            row_values = (art_id, price, collection_id, collection_name, contract_token, event_date, time, blockchain, event_type, buyer, seller, contract_address)
            cur.execute(
                'INSERT INTO `nft_events`.`open_sea_events` (`art_id`, `price`, `collection_id`, `collection_name`,`contract_token_id`, `event_date`, `created_date`, `blockchain`, `event_type`,`buyer`,`seller`,`collection_address`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s)',
                row_values)
            conn.commit()
            log.info("rds submitted")

    except Exception as e:
        log.info(e)

    return art_record


