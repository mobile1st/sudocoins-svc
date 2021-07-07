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
        log.info(f'user_id: {user_id} url: {input_url}')

        contract_id, token_id = parse_url(input_url)
        open_sea_response = call_open_sea(contract_id, token_id)

        return add(contract_id, token_id, open_sea_response, input_url, user_id)
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
    # BEGIN register_art(contract_id, token_id, open_sea_response, input_url) -> art
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
        "creator": open_sea_response['creator'],
        "permalink": open_sea_response['permalink']
    }

    preview_url, art_url = get_urls(open_sea)

    if open_sea['permalink'] is None:
        buy_url = input_url
    else:
        buy_url = open_sea['permalink']

    art_object = dynamodb.Table('art').query(
        KeyConditionExpression=Key('contractId#tokenId').eq(contract_token_id),
        ScanIndexForward=False,
        IndexName='Art_dedupe_idx')

    if not art_object['Count'] > 0:
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
            "first_user": user_id,
            "sort_idx": 'true',
            "creator": open_sea['creator'],
            "process_status": "0"
        }
        dynamodb.Table('art').put_item(
            Item=art_record
        )
        sns_client = boto3.client("sns")
        sns_client.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
            MessageStructure='string',
            MessageAttributes={
                'art_id': {
                    'DataType': 'String',
                    'StringValue': art_id
                },
                'art_url': {
                    'DataType': 'String',
                    'StringValue': art_url
                }
            },
            Message=json.dumps(art_record)
        )
        log.info("art pushed to sns")


    elif art_object['Count'] > 0:
        art_id = art_object['Items'][0]['art_id']

    # END register_art(contract_id, token_id, open_sea_response, input_url) -> art

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
        "timestamp": time_now,
        "dedupe_key": dedupe_key,
        "art_id": art_id,
        'creator': open_sea['creator']
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

    art.addLedgerRecord(5, user_id, 'Add Art')

    return {
        'status': 'success',
        'shareId': art_uploads_record['shareId'],
        'balance': new_sudo['Attributes']['sudocoins']
    }


def get_urls(open_sea):
    anim_url: str = open_sea['animation_url']

    # https://api.artblocks.io/generator/ is an html image generator and cannot be embedded directly
    if anim_url and (not anim_url.startswith('https://api.artblocks.io/generator/')):
        return open_sea["image_preview_url"],  open_sea["animation_original_url"]

    if open_sea['image_original_url'] is None:
        return open_sea["image_preview_url"], open_sea['image_url']

    return open_sea["image_preview_url"], open_sea['image_original_url']
