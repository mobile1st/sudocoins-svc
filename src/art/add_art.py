import boto3
import json
import re
import http.client
import uuid
from boto3.dynamodb.conditions import Key
from datetime import datetime

from art.art import Art
from util import sudocoins_logger
from art.ledger import Ledger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
ledger = Ledger(dynamodb)
art = Art(dynamodb)

# (user_id, url) -> {
#   status: 'exist' | 'fail' | 'success'
#   shareId: string uuid, mandatory for 'exist' and 'success'
#   balance: integer, optional new balance
#   message: string, optional message
# }
def lambda_handler(event, context):
    try:
        set_log_context(event)
        log.debug(f'event: {event}')
        body = json.loads(event.get('body', '{}'))
        input_url = body['url']
        user_id = body['userId']
        tags = body.get("tags")
        log.info(f'user_id: {user_id} url: {input_url} tags: {tags}')

        contract_id, token_id = parse_url(input_url)
        open_sea_response = call_open_sea(contract_id, token_id)

        return add(contract_id, token_id, open_sea_response, input_url, user_id, tags)
    except Exception as e:
        log.exception(e)
        return {
            'status': 'fail',
            'message': "Sorry, your input doesn't return any Art. Please Add another Art."
        }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def parse_url(url):
    contract_id = ''
    token_id = ''
    if url.find('rarible.com') != -1:
        sub1 = url.find('token/')
        start = sub1 + 6
        rest = url[start:]
        variables = re.split(r':|\?', rest)
        log.debug(f'variables {variables}')
        contract_id = variables[0]
        token_id = variables[1]
    elif url.find('opensea.io') != -1:
        sub1 = url.find('assets/')
        start = sub1 + 7
        rest = url[start:]
        variables = rest.split('/')
        log.debug(f'variables {variables}')
        contract_id = variables[0]
        token_id = variables[1]
    elif url.find('foundation.app') != -1:
        contract_id = '0x3B3ee1931Dc30C1957379FAc9aba94D1C48a5405'
        variables = url.split('-')
        chunks = len(variables)
        token_id = variables[chunks-1]
    elif url.find('axieinfinity.com') != -1:
        contract_id = '0xf5b0a3efb8e8e4c201e2a935f110eaaf3ffecb8d'
        variables = url.split('/')
        chunks = len(variables)
        token_id = variables[chunks - 1]
    elif url.find('larvalabs.com') != -1:
        contract_id = '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
        variables = url.split('/')
        chunks = len(variables)
        token_id = variables[chunks - 1]
    elif url.find('superrare.com') != -1:
        contract_id = '0xb932a70a57673d89f4acffbe830e8ed7f75fb9e0'
        variables = url.split('-')
        chunks = len(variables)
        token_id = variables[chunks - 1]
    elif url.find('zora.co') != -1:
        contract_id = '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7'
        variables = url.split('/')
        chunks = len(variables)
        token_id = variables[chunks - 1]

    log.debug(f'contract_id: {contract_id}, token_id: {token_id}')
    return contract_id, token_id




def call_open_sea(contract_id, token_id):
    open_sea_url_pattern = "/api/v1/asset/{0}/{1}"
    path = open_sea_url_pattern.format(contract_id, token_id)
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    open_sea_response = json.loads(response.read())
    log.info(f'open_sea_response: {open_sea_response}')
    return open_sea_response


def add(contract_id, token_id, open_sea_response, input_url, user_id, tags):
    open_sea = {
        'redirect': input_url,
        'name': open_sea_response['name'],
        'description': open_sea_response['description'],
        "image_url": open_sea_response['image_url'],
        "image_preview_url": open_sea_response['image_preview_url'],
        "image_thumbnail_url": open_sea_response['image_thumbnail_url'],
        "image_original_url": open_sea_response['image_original_url'],
        "animation_url": open_sea_response['animation_url'],
        "animation_original_url": open_sea_response['animation_original_url'],
        "creator": open_sea_response['creator'],
        "permalink": open_sea_response['permalink']
    }

    preview_url, art_url = get_urls(open_sea)
    buy_url = open_sea['permalink'] if open_sea.get('permalink') else input_url
    art_id = get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, user_id, tags)

    # BEGIN register_art_upload(art, user_id) -> art_upload
    # check to see if art_uploads record already exists
    dedupe_key = str(user_id) + '#' + str(contract_id) + "#" + str(token_id)

    art_uploads_object = dynamodb.Table('art_uploads').query(
        KeyConditionExpression=Key("dedupe_key").eq(dedupe_key),
        IndexName='User_upload_dedupe_idx')

    if art_uploads_object['Count'] > 0:
        return {
            'status': 'exist',
            'shareId': art_uploads_object['Items'][0]['shareId']
        }
    creator_address = open_sea['creator'].get('address') if open_sea.get('creator') else "unknown"
    art_uploads_record = {
        "shareId": str(uuid.uuid1()),
        'contractId#tokenId': str(contract_id) + "#" + str(token_id),
        "name": open_sea['name'],
        "buy_url": buy_url,
        "user_id": user_id,
        'preview_url': preview_url,
        'art_url': art_url,
        "open_sea_data": open_sea,
        "click_count": 0,
        "timestamp": str(datetime.utcnow().isoformat()),
        "dedupe_key": dedupe_key,
        "art_id": art_id,
        'creator': creator_address
    }
    dynamodb.Table('art_uploads').put_item(
        Item=art_uploads_record
    )
    # END register_art_upload(art, user_id) -> art_upload

    # CALL profile.add_sudo(user_id, 5) -> sudo (integer, the new balance in sudo)
    new_sudo = dynamodb.Table('Profile').update_item(
        Key={'userId': user_id},
        UpdateExpression="SET sudocoins = if_not_exists(sudocoins, :start) + :inc",
        ExpressionAttributeValues={
            ':inc': 5,
            ':start': 0
        },
        ReturnValues="UPDATED_NEW"
    )
    ledger.add(5, user_id, 'Add Art')

    try:
        dynamodb.Table('creators').put_item(
            Item={
                'address': open_sea_response['creator']['address'],
                'open_sea_data': open_sea_response['creator'],
                'timestamp': str(datetime.utcnow().isoformat()),
                'last_update': str(datetime.utcnow().isoformat())
            },
            ConditionExpression='attribute_not_exists(address)'
        )
    except Exception as e:
        log.info(e)

    return {
        'status': 'success',
        'shareId': art_uploads_record['shareId'],
        'balance': new_sudo['Attributes']['sudocoins']
    }


def get_art_id(contract_id, token_id, art_url, buy_url, preview_url, open_sea, user_id, tags):
    contract_token_id = str(contract_id) + "#" + str(token_id)
    art_id = art.get_id(contract_token_id)
    if art_id:
        return art_id

    return art.add(contract_token_id, art_url, preview_url, buy_url, open_sea, user_id, tags)['art_id']


def get_urls(open_sea):
    if open_sea.get('animation_original_url'):
        return open_sea["image_preview_url"],  open_sea["animation_original_url"]

    if open_sea.get('image_original_url'):
        return open_sea["image_preview_url"], open_sea['image_original_url']

    return open_sea["image_preview_url"], open_sea['image_url']
