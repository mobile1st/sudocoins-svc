import boto3
import json
from art.art import Art
from util import sudocoins_logger
from decimal import Decimal, getcontext
import pymysql
from datetime import datetime
import uuid
import os

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")
art = Art(dynamodb)
sns = boto3.client("sns")


def lambda_handler(event, context):
    art_object = json.loads(event['Records'][0]['Sns']['Message'])
    # log.info(f'payload: {art_object}')

    if art_object['open_sea_url'] is None:
        log.info("open sea url doesn't exist")
        return

    try:
        response = add(art_object)
        if response is not None:
            log.info('status: success')
    except Exception as e:
        log.info(f"status: failure - {e}")

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


def get_urls(open_sea):
    if open_sea.get('animation_original_url'):
        return open_sea["image_preview_url"], open_sea["animation_original_url"]

    if open_sea.get('image_original_url'):
        return open_sea["image_preview_url"], open_sea['image_original_url']

    return open_sea["image_preview_url"], open_sea['image_url']


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def add(art_object):
    collection_address, token_id = parse_url(art_object['open_sea_url'])
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
        "token_metadata": art_object.get('asset', {}).get('token_metadata')
    }
    buy_url = open_sea['permalink'] if open_sea.get('permalink') else art_object.get('open_sea_url')
    preview_url, art_url = get_urls(open_sea)
    eth_sale_price = eth_price(art_object)

    # function responsible for adding and inserting
    response = get_art_id(collection_address, token_id, art_url, buy_url, preview_url, open_sea, art_object,
                          eth_sale_price)

    return response


def get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price):
    contract_token_id = str(contract_id) + "#" + str(token_id)
    nft_id = art.get_id(contract_token_id)
    if nft_id:
        try:
            update_nft(nft_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price, contract_token_id)
            collection_id = insert_rds(nft_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price,
                                       contract_token_id)
            if collection_id is None:
                log.info("collection_id is None")
                return
            update_collection(art_object, eth_sale_price, collection_id)
        except Exception as e:
            log.info(f"status: failure - {e}")

        return

    else:
        nft_id = str(uuid.uuid1())
        try:
            add_nft(nft_id, contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price)
            collection_id = insert_rds(nft_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price,
                                       contract_token_id)
            if collection_id is None:
                log.info("collection_id is None")
                return
            update_collection(art_object, eth_sale_price, collection_id)
        except Exception as e:
            log.info(f"status: failure - {e}")

        return


def insert_rds(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price, contract_token_id):
    time_now = str(datetime.utcnow().isoformat())
    art_record = {
        'art_id': art_id,
        "name": open_sea['name'],
        'buy_url': buy_url,
        'contractId#tokenId': contract_token_id,
        'preview_url': preview_url,
        'art_url': art_url,
        "open_sea_data": open_sea,
        "timestamp": time_now,
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
        "collection_name": art_object.get('asset', {}).get('collection', {}).get('name'),
        "owner": art_object.get("owner", "unknown"),
        "seller": art_object.get("seller", "unknown")
    }

    if art_record.get('blockchain') == 'Ethereum':
        blockchain_id = 1
        currency_id = 1
    elif art_record.get('blockchain') == 'polygon':
        blockchain_id = 2
        currency_id = 2

    if art_record.get('event_type') == 'successful':
        event_id = 1

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
        rds_host = os.environ['db_host']
        name = os.environ['db_user']
        password = os.environ['db_pw']
        db_name = os.environ['db_name']
        log.info("about to connect")

        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)
        log.info("conn object made")
        with conn.cursor() as cur:
            log.info("cur established")
            # collection table
            collection_code = art_record['collection_id']

            sql = '''select id, collection_url from nft.collections where collection_code=%s limit 1;'''
            cur.execute(sql, collection_code)
            result = cur.fetchall()
            log.info(result)

            if len(result) == 0:
                log.info("new collection")
                name = art_record.get('collection_name')
                avatar = art_object.get('asset', {}).get('collection', {}).get('image_url')
                collection_address = art_record.get("collection_address")
                created_date = art_object.get('collection_date')
                row_values = (collection_code, name, avatar, collection_address, created_date, blockchain_id)
                cur.execute(
                    'INSERT INTO `nft`.`collections` (`collection_code`, `collection_name`, `avatar`, `collection_address`,`created_date`, `blockchain_id`) VALUES (%s, %s, %s, %s, %s,%s)',
                    row_values)

                sql = '''select id from nft.collections where collection_code=%s limit 1;'''
                cur.execute(sql, collection_code)
                result = cur.fetchall()
                collection_id = result[0][0]
            elif len(result) == 1 and result[0][1] is None:
                log.info("update collection_url")
                collection_url = art_object.get('asset', {}).get('collection', {}).get('slug', "")
                sql = '''update nft.collections set collection_url=%s where id=%s;'''
                cur.execute(sql, (collection_url, result[0][0]))
                collection_id = result[0][0]
            else:
                log.info("collection_url exists")
                collection_id = result[0][0]
            # nft
            nft_code = art_record['art_id']
            token_id = int(art_record.get('contractId#tokenId').split('#')[1])
            sql = '''select id from nft.nfts where art_code=%s limit 1;'''
            cur.execute(sql, nft_code)
            result = cur.fetchall()
            if len(result) == 0:
                row_values = (nft_code, collection_id, token_id, art_record.get("preview_url"))
                cur.execute(
                    'INSERT INTO `nft`.`nfts` (`art_code`, `collection_id`, `token_id`, `avatar`) VALUES (%s,%s,%s,%s)',
                    row_values)
                sql = '''select id from nft.nfts where art_code=%s limit 1;'''
                cur.execute(sql, nft_code)
                result = cur.fetchall()
                nft_id = result[0][0]
            else:
                nft_id = result[0][0]
            # buyer
            public_key = art_record['owner']
            sql = '''select id from nft.users where public_key=%s limit 1;'''
            cur.execute(sql, public_key)
            result = cur.fetchall()
            if len(result) == 0:
                row_values = (public_key)
                cur.execute(
                    'INSERT INTO `nft`.`users` (`public_key`) VALUES (%s)',
                    row_values)
                sql = '''select id from nft.users where public_key=%s limit 1;'''
                cur.execute(sql, public_key)
                result = cur.fetchall()
                buyer_id = result[0][0]
            else:
                buyer_id = result[0][0]
            # seller
            public_key = art_record['seller']
            sql = '''select id from nft.users where public_key=%s limit 1;'''
            cur.execute(sql, public_key)
            result = cur.fetchall()
            if len(result) == 0:
                row_values = (public_key)
                cur.execute(
                    'INSERT INTO `nft`.`users` (`public_key`) VALUES (%s)', row_values)
                sql = '''select id from nft.users where public_key=%s limit 1;'''
                cur.execute(sql, public_key)
                result = cur.fetchall()
                seller_id = result[0][0]
            else:
                seller_id = result[0][0]

            price = int(art_record['last_sale_price'])
            event_date = art_record['event_date']
            row_values = (
                collection_id, nft_id, price, event_date, time_now, blockchain_id, event_id, buyer_id, seller_id,
                currency_id)
            cur.execute(
                'INSERT INTO `nft`.`events` (`collection_id`,`nft_id`,`price`, `event_date`, `created_date`, `blockchain_id`, `event_id`,`buyer_id`,`seller_id`, `currency`) VALUES (%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)',
                row_values)
            conn.commit()
            conn.close()
            log.info("rds submitted and connection closed")


    except Exception as e:
        error = str(e)
        if "Duplicate entry" in error:
            return None
        log.info(f"status: failure - {e}")
        conn.close()
        return None

    return collection_id


def eth_price(art_object):
    getcontext().prec = 18
    symbol = art_object.get("payment_token", {})
    if symbol is None:
        symbol = "ETH"
    else:
        symbol = symbol.get("symbol", "ETH")

    if symbol != 'ETH':
        total_price = Decimal(art_object.get("sale_price", 0))
        eth_price = Decimal(art_object.get("payment_token", {}).get("eth_price", "1"))
        eth_sale_price = int(total_price * eth_price)

    else:
        eth_sale_price = int(art_object.get("sale_price", 0))

    return eth_sale_price


def add_nft(nft_id, contract_token_id, art_url, preview_url, buy_url, open_sea, art_object, eth_sale_price):
    dynamodb = boto3.resource('dynamodb')
    time_now = str(datetime.utcnow().isoformat())
    art_record = {
        'art_id': nft_id,
        "name": open_sea['name'],
        'buy_url': buy_url,
        'contractId#tokenId': contract_token_id,
        'preview_url': preview_url,
        'art_url': art_url,
        "open_sea_data": open_sea,
        "recent_sk": time_now + "#" + nft_id,
        "token_id": contract_token_id.split("#")[1],
        "sort_idx": 'true',
        "event_date": art_object.get('created_date'),
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
        "collection_name": art_object.get('asset', {}).get('collection', {}).get('name')
    }

    if art_record['collection_name'] is not None and art_record['collection_address'] is not None:
        c_name = ("-".join(art_record['collection_name'].split())).lower()
        art_record['collection_id'] = art_record['collection_address'] + ":" + c_name
        collection_url = art_object.get('asset', {}).get('collection', {}).get('slug', "")
        collection_item_id = contract_token_id.split("#")[1]
        collection_item_url = collection_url + "-" + collection_item_id
        art_record['collection_url'] = collection_url
        art_record['collection_item_url'] = collection_item_url
    else:
        art_record['collection_id'] = art_record['collection_address']

    if art_record['preview_url'] is None:
        art_record['preview_url'] = art_record['art_url']

    if 'name' in art_record and art_record['name'] is None:
        name = art_record.get('collection_data', {}).get('name', "")
        number = art_record.get("contractId#tokenId", "")
        number = number.split('#')[1]
        art_record['name'] = name + " #" + str(number)

    if art_url != "" and preview_url != "":
        dynamodb.Table('art').put_item(Item=art_record)
        log.info("art added to art table")

    '''
    try:

        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
            MessageStructure='string',
            MessageAttributes={
                'art_id': {
                    'DataType': 'String',
                    'StringValue': nft_id
                },
                'art_url': {
                    'DataType': 'String',
                    'StringValue': art_url
                },
                'process': {
                    'DataType': 'String',
                    'StringValue': "STREAM_TO_S3"
                }
            },
            Message=json.dumps(art_record)
        )
    except Exception as e:
        log.info(f"open_sea {open_sea}")
        log.info(f"status: failure - {e}")
    '''
    '''
    try:
        msg = {
            "collection_id": art_record.get('collection_id', "")
        }
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:AddSearchTopic',
            MessageStructure='string',
            Message=json.dumps(msg)
        )
        log.info(f"add search message published")
    except Exception as e:
        log.info(f"status: failure - {e}")
    '''


def update_nft(art_id, art_url, buy_url, preview_url, open_sea, art_object, eth_sale_price, contract_token_id):
    dynamodb = boto3.resource('dynamodb')
    collection_address = art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown")
    collection_name = art_object.get('asset', {}).get('collection', {}).get('name')
    c_name = ("-".join(collection_name.split())).lower()
    collection_id = collection_address + ":" + c_name
    collection_url = art_object.get('asset', {}).get('collection', {}).get('slug', "")
    collection_item_id = contract_token_id.split("#")[1]
    collection_item_url = collection_url + "-" + collection_item_id

    if art_url != "" and preview_url is not None:
        dynamodb.Table('art').update_item(
            Key={'art_id': art_id},
            UpdateExpression="SET art_url=:art, preview_url=:pre, "
                             "last_sale_price=:lsp,"
                             "collection_url=:curl, token_id=:tok,"
                             "collection_item_url=:ciurl, collection_id=:cid",
            ExpressionAttributeValues={
                ':art': art_url,
                ':curl': collection_url,
                ':ciurl': collection_item_url,
                ':pre': preview_url,
                ':tok': contract_token_id.split("#")[1],
                ':lsp': eth_sale_price,
                ':cid': collection_id
            }
        )
        log.info("art record updated")
        log.info(f"art_id: {art_id}")
    else:
        log.info("missing art_url and preview_url")
        return


def update_collection(art_object, eth_sale_price, collection_id):
    collection_address = art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown")
    collection_name = art_object.get('asset', {}).get('collection', {}).get('name')
    c_name = ("-".join(collection_name.split())).lower()
    collection_code = collection_address + ":" + c_name

    remove_chars = [',', '&', '+', '.', '!', '(', ')', '`', '"', "'"]
    replace_chars = [' ', '#', '/', '-', ':', '--', "|"]
    collection_url = collection_name.lower()
    for i in remove_chars:
        collection_url = collection_url.replace(i, '')
    for i in replace_chars:
        collection_url = collection_url.replace(i, '-')

    try:
        msg = {
            "last_sale_price": eth_sale_price,
            "collection_id": collection_id,
            'art_object': art_object,
            'collection_code': collection_code,
            'collection_url': collection_url,
            "twitter": art_object.get('asset', {}).get('collection', {}).get('twitter_username', ""),
            "end_time": art_object.get("end_time")
        }

        sns_client.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:AddTimeSeries2Topic',
            MessageStructure='string',
            Message=json.dumps(msg)
        )

        log.info(f"add time series published")
    except Exception as e:
        log.info(f"status: failure - {e}")



