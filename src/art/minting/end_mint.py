import boto3
from util import sudocoins_logger
from datetime import datetime
import json



log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    body = json.loads(event['body'])
    # body = event
    time_now = str(datetime.utcnow().isoformat())
    url = "https://rarible.com/token/"

    # contractId = body['make']['assetType']['contract']
    contractId = body['contract']
    tokenId = body['tokenId']
    contract_token_id = contractId + "#" + tokenId
    contract_token_id_rarible = contractId + ":" + tokenId

    art_parse = body['file_name'];
    art_id = art_parse.split('.')[0]
    
    creator = body['account']
    owner = creator;
    uri = body['uri'];
    price = body['price'];

    art_record = {
        'art_id': art_id,
        "name": body.get('name', ""),
        "description": body.get('description', ""),
        'buy_url': url + contract_token_id_rarible,
        'contractId#tokenId': contract_token_id,
        'preview_url': "https://cdn.sudocoins.com/" + body.get("file_name"),
        'art_url': "https://cdn.sudocoins.com/" + body.get("file_name"),
        "timestamp": time_now,
        "recent_sk": time_now + "#" + art_id,
        "click_count": 0,
        "first_user": creator,
        "sort_idx": 'true',
        "creator": creator,
        "process_status": "STREAM_TO_S3",
        "event_date": "0",
        "event_type": "mint",
        "blockchain": "Ethereum",
        "last_sale_price": 0,
        "collection_address": contractId,
        "collection_data": {
            "name": "Rarible",
            "image_url": "https://lh3.googleusercontent.com/FG0QJ00fN3c_FWuPeUr9-T__iQl63j9hn5d6svW8UqOmia5zp3lKHPkJuHcvhZ0f_Pd6P2COo9tt9zVUvdPxG_9BBw=s60"
        },
        "collection_name": "Rarible",
        "process_to_google_search": "TO_BE_INDEXED",
        "metadata": uri,
        "royalty": body['royalty'],
        "list_price": price,
        "owner": owner
    }

    c_name = ("-".join(art_record['collection_name'].split())).lower()
    art_record['collection_id'] = art_record['collection_address'] + ":" + c_name

    dynamodb.Table('art').put_item(Item=art_record)

    try:
        dynamodb.Table('collections').update_item(
            Key={
                'collection_id': art_record['collection_id']
            },
            UpdateExpression="SET sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(sale_volume, :start2) + :inc2,"
                             "collection_name = :cn, preview_url = :purl",
            ExpressionAttributeValues={
                ':start': 0,
                ':inc': 1,
                ':start': 0,
                ':inc': art_record['last_sale_price'],
                ':cn': art_record['collection_name'],
                ':purl': art_record['preview_url']
            },
            ReturnValues="UPDATED_NEW"
        )
    except Exception as e:
        log.info(e)

    return {
        "art_id": art_record['art_id']
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))



