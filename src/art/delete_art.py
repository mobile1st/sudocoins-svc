import boto3
from botocore.config import Config
import json
from util import sudocoins_logger
from ethereum.utils import ecrecover_to_pub, sha3
from eth_utils.hexadecimal import encode_hex, decode_hex, add_0x_prefix
from boto3.dynamodb.conditions import Key
from art.art import Art


log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    jsonInput = json.loads(event.get('body', '{}'))

    sub = jsonInput.get('sub', "")
    art_id = jsonInput.get('art_id', "")
    publicAddress = jsonInput.get('public_address', "")
    signature = jsonInput.get('signature', "")
    hash_message = jsonInput.get('hash_message', "")

    if publicAddress != "":
        try:
            verify_delete(sub, art_id, publicAddress, signature, hash_message)

        except Exception as e:
            log.exception(e)

    # return {
    #     "arts": get_uploads(sub)
    # }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def verify_delete(sub, art_id, publicAddress, signature, hash_message):

    subTable = dynamodb.Table('sub')
    artTable = dynamodb.Table('art')
    subResponse = subTable.get_item(Key={'sub': publicAddress})
    log.info(f'subResponse: {subResponse}')

    log.info(f'msgHex: {hash_message}')

    r = int(signature[0:66], 16)
    s = int(add_0x_prefix(signature[66:130]), 16)
    v = int(add_0x_prefix(signature[130:132]), 16)
    if v not in (27, 28):
        v += 27
    pubkey = ecrecover_to_pub(decode_hex(hash_message), v, r, s)
    log.info(f'publicAddress: {publicAddress}')
    log.info(f'pubkey: {encode_hex(sha3(pubkey)[-20:])}')

    if publicAddress == encode_hex(sha3(pubkey)[-20:]):

        artTable.delete_item(Key={'art_id': art_id})
        return {
            "arts": get_uploads(sub)
        }
    else:
        return {
            'status': 404,
            'message': 'Invalid signature'
        }


def get_uploads(sub):
    # returns the user's uploaded art sorted by timestamp
    uploads = dynamodb.Table('art').query(
        KeyConditionExpression=Key('creator').eq(sub),
        ScanIndexForward=False,
        IndexName='creators-index',
        ExpressionAttributeNames={'#n': 'name'},
        ProjectionExpression='shareId, click_count, art_url, art_id, preview_url, #n, tags, last_sale_price, list_price'
    )['Items']

    art_ids = [i['art_id'] for i in uploads]
    art_list = arts.get_arts(art_ids)

    art_index = {}
    for art in art_list:
        art_index[art['art_id']] = art

    sanitized = []  # remove art that is no longer present in the art table
    for a in uploads:
        idx = art_index.get(a['art_id'])
        if not idx:
            log.info(f'Could not find art_id {a["art_id"]} in art table, hiding from user arts too')
            continue

        a['art_url'] = idx['art_url']  # this maybe a cdn url
        if idx.get('mime_type'):
            a['mime_type'] = idx.get('mime_type')
            a['last_sale_price'] = idx.get('last_sale_price')
        sanitized.append(a)

    return sanitized

