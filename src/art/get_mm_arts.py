import boto3
from boto3.dynamodb.conditions import Key
from util import sudocoins_logger
from art.art import Art
import http.client
import json
import uuid
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    event = json.loads(event['Records'][0]['Sns']['Message'])
    set_log_context(event)
    sub = event['public_address']
    log.info(f"public_address {sub}")

    nfts = get_metamask_arts(sub)

    for i in nfts:
        get_art_id(i)

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_metamask_arts(public_address):
    try:
        path = "https://api.opensea.io/api/v1/assets?owner=" + public_address + "&limit=50&offset=" + "0"
        log.info(f'path: {path}')
        conn = http.client.HTTPSConnection("api.opensea.io")
        conn.request("GET", path)
        response = conn.getresponse()
        response2 = response.read().decode('utf-8')
        open_sea_response = json.loads(response2)['assets']

        arts = []
        arts = arts + open_sea_response

        try:
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

                get_art_id(contract_token)

                open_sea = {
                    "image_url": i.get('image_url'),
                    "image_preview_url": i.get('image_preview_url'),
                    "image_thumbnail_url": i.get('image_thumbnail_url'),
                    "image_original_url": i.get('image_original_url'),
                    "animation_url": i.get('animation_url'),
                    "animation_original_url": i.get('animation_original_url')
                }

                msg = {
                    "art_id": str(uuid.uuid1()),
                    "art_url": i.get("image_url", ""),
                    "preview_url": i.get("image_preview_url", ""),
                    "name": i.get("name", ""),
                    "description": i.get("description", ""),
                    "collection_address": i.get("asset_contract", {}).get("address", ""),
                    "collection_data": {
                        "name": i.get("asset_contract", {}).get("address", ""),
                        "description": i.get("asset_contract", {}).get("description", "")
                    },
                    "collection_name": i.get("collection", {}).get("name", ""),
                    "contractId#tokenId": contract_token,
                    "buy_url": i.get("permalink", ""),
                    "open_sea_data": open_sea,
                    "owner": public_address
                }

                if i['last_sale'] is not None:
                    msg['last_sale_price'] = i.get('last_sale', {}).get('total_price')

                if i['sell_orders'] is not None and len(i['sell_orders']) > 0:
                    msg['list_price'] = i.get('sell_orders')[0]['current_price']

                art_objects.append(msg)

        except Exception as e:
            log.info(e)

    except Exception as e:
        log.info(e)

    return art_objects


def get_art_id(contract_token_id, msg):
    art_id = art.get_id(contract_token_id)
    if art_id:
        return update_art(art_id, msg)

    return add_art(msg)


def update_art(art_id, msg):

    dynamodb.Table('art').update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET #o=:ow",
        ExpressionAttributeValues={

            ":ow": msg['owner']
        },
        ExpressionAttributeNames={'#o': 'owner'}
    )

    log.info("art record updated")
    log.info(f"art_id: {art_id}")

    return


def add_art(msg):
    dynamodb = boto3.resource('dynamodb')
    sns = boto3.client("sns")
    time_now = str(datetime.utcnow().isoformat())
    art_id = str(uuid.uuid1())
    log.info(f"art.add {msg} {art_id}")
    art_record = {
        'art_id': art_id,
        "name": msg['name'],
        'buy_url': msg['buy_url'],
        'contractId#tokenId': msg['contractId#tokenId'],
        'preview_url': msg['preview_url'],
        'art_url': msg['art_url'],
        "open_sea_data": msg['open_sea'],
        "timestamp": time_now,
        "recent_sk": time_now + "#" + art_id,
        "click_count": 0,
        "first_user": "metamask",
        "sort_idx": 'true',
        "process_status": "STREAM_TO_S3",
        "event_date": "",
        "event_type": "metamask",
        "blockchain": "Ethereum",
        "last_sale_price": msg['last_sale_price'],
        "collection_address": msg['collection_address'],
        "collection_data": msg['collection_data'],
        "process_to_google_search": "TO_BE_INDEXED",
        "collection_name": msg['collection_name'],
        "owner": msg['owner'],
        "seller": "unknown"
    }

    if art_record['collection_name'] is not None and art_record['collection_address'] is not None:
        c_name = ("-".join(art_record['collection_name'].split())).lower()
        art_record['collection_id'] = art_record['collection_address'] + ":" + c_name
    else:
        art_record['collection_id'] = art_record['collection_address']

    log.info(f"art.add {art_record}")
    if art_record['preview_url'] is None:
        art_record['preview_url'] = art_record['art_url']

    if 'name' in art_record and art_record['name'] is None:
        name = art_record.get('collection_data', {}).get('name', "")
        number = art_record.get("contractId#tokenId", "")
        number = number.split('#')[1]
        art_record['name'] = name + " #" + str(number)

    dynamodb.Table('art').art_table.put_item(Item=art_record)

    try:
        sns.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
            MessageStructure='string',
            MessageAttributes={
                'art_id': {
                    'DataType': 'String',
                    'StringValue': art_id
                },
                'art_url': {
                    'DataType': 'String',
                    'StringValue': art_record['art_url']
                },
                'process': {
                    'DataType': 'String',
                    'StringValue': "STREAM_TO_S3"
                }
            },
            Message=json.dumps(art_record)
        )
    except Exception as e:
        log.info(f"art_record {art_record}")
        log.info(e)


    return art_record
