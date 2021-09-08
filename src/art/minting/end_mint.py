import boto3
from util import sudocoins_logger
from datetime import datetime
import uuid
import json


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event['body'])
    time_now = str(datetime.utcnow().isoformat())

    url = "https://rarible.com/token/"
    ending = body.get("mint_response", {}).get("id")

    contractId = body.get("mint_response", {}).get("id").split(":")[0]
    tokenId = body.get("mint_response", {}).get("id").split(":")[1]

    contract_token_id = contractId + "#" + tokenId

    art_parse = body.get("file_name")
    art_id = art_parse.split('.')[0]

    creator = body.get("mint_response", {}).get("creators")[0]['account']

    short_code = str(uuid.uuid4())[:8]

    art_record = {
        'art_id': art_id,
        "name": body.get("form_data", {}).get('name'),
        "description": body.get("form_data", {}).get('description'),
        'buy_url': url + ending,
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
        "tags": [],
        "event_date": time_now,
        "event_type": "mint",
        "blockchain": "Ethereum",
        "last_sale_price": 0,
        "collection_address": contractId,
        "collection_data": {
            "name": "Rarible",
            "image_url": "https://lh3.googleusercontent.com/FG0QJ00fN3c_FWuPeUr9-T__iQl63j9hn5d6svW8UqOmia5zp3lKHPkJuHcvhZ0f_Pd6P2COo9tt9zVUvdPxG_9BBw=s60"
        },
        "process_to_google_search": "TO_BE_INDEXED",
        "metadata": body.get("mint_response", {}).get('uri'),
        "royalty": body.get("mint_response", {}).get('royalties'),
        "short_code": short_code
    }

    dynamodb.Table('art').put_item(Item=art_record)

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))



