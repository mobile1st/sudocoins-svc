import boto3
import json
import re
import http.client
import uuid
from boto3.dynamodb.conditions import Key
from datetime import datetime
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    try:
        body = json.loads(event['body'])
        input_url = body['url']
        user_id = body['userId']

        contract_id, token_id = parse_url(input_url)
        open_sea_response = call_open_sea(contract_id, token_id)

        return add(contract_id, token_id, open_sea_response, input_url, user_id)
    except Exception as e:
        log.exception(e)
        return {
            "error": True,
            "msg": "Sorry, your input doesn't return any Art. Please Add another Art."
        }


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


def add(contract_id, token_id, open_sea_response, input_url, user_id):
    time_now = str(datetime.utcnow().isoformat())
    contract_token_id = str(contract_id) + "#" + str(token_id)

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
        "creator": open_sea_response['creator']
    }

    if open_sea['animation_url'] is None:
        preview_url = open_sea["image_preview_url"]
        art_url = open_sea["image_url"]
    else:
        preview_url = open_sea["image_preview_url"]
        art_url = open_sea['animation_original_url']

    art_object = dynamodb.Table('art').query(
        KeyConditionExpression=Key('contractId#tokenId').eq(contract_token_id),
        ScanIndexForward=False,
        IndexName='Art_dedupe_idx')

    if not art_object['Count'] > 0:
        art_id = str(uuid.uuid1())
        art_record = {
            'art_id': art_id,
            "name": open_sea['name'],
            'buy_url': input_url,
            'contractId#tokenId': contract_token_id,
            'preview_url': preview_url,
            'art_url': art_url,
            "open_sea_data": open_sea,
            "timestamp": time_now,
            "recent_sk": time_now + "#" + art_id,
            "click_count": 0,
            "first_user": user_id,
            "sort_idx": 'true'
        }
        dynamodb.Table('art').put_item(
            Item=art_record
        )
    elif art_object['Count'] > 0:
        art_id = art_object['Items'][0]['art_id']

    # check to see if art_uploads record already exists

    dedupe_key = str(user_id) + '#' + str(contract_id) + "#" + str(token_id)

    art_uploads_object = dynamodb.Table('art_uploads').query(
        KeyConditionExpression=Key("dedupe_key").eq(dedupe_key),
        IndexName='User_upload_dedupe_idx')

    if art_uploads_object['Count'] > 0:
        msg = {
            "message": "Art already added to Gallery",
            "shareId": art_uploads_object['Items'][0]['shareId']
        }

        return msg

    else:
        art_uploads_record = {
            "shareId": str(uuid.uuid1()),
            'contractId#tokenId': str(contract_id) + "#" + str(token_id),
            "name": open_sea['name'],
            "buy_url": input_url,
            "user_id": user_id,
            'preview_url': preview_url,
            'art_url': art_url,
            "open_sea_data": open_sea,
            "click_count": 0,
            "timestamp": time_now,
            "dedupe_key": dedupe_key,
            "art_id": art_id
        }
        dynamodb.Table('art_uploads').put_item(
            Item=art_uploads_record
        )

        new_sudo = dynamodb.Table('Profile').update_item(
            Key={'userId': user_id},
            UpdateExpression="SET sudocoins = if_not_exists(sudocoins, :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 5,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        sudo = new_sudo['Attributes']['sudocoins']
        art_uploads_record['sudocoins'] = sudo

        art.addLedgerRecord(5, user_id, 'Add Art')

        return art_uploads_record
