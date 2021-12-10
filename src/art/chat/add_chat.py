import boto3
from util import sudocoins_logger
import json
from datetime import datetime
import uuid
import http.client
from ethereum.utils import ecrecover_to_pub, sha3
from eth_utils.hexadecimal import encode_hex, decode_hex, add_0x_prefix
import http.client

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')
    '''
    try:
        state = verifyPubKey(body.get(publicAddress), body.get(signature), body.get(hash_message))
    except Exception as e:
        state = False

    if state == False:
        return {
            'status': 404,
            'message': 'Invalid authentication.'
        }
    
    state2 = verifyNFTowner(body.get(publicAddress), body.get(contractAddress))
    if state2 == False:
        return {
            'status': 404,
            'message': 'User doesn't own NFTs from this collection'
        }
    
    '''
    if body.get("detail", {}).get("type", "") == "message":
        try:
            captchaToken = body.get("captchaToken")
            recaptcha_response = call_google_recaptcha(captchaToken)
            success_response = recaptcha_response['success']
            if success_response is False:
                return

        except Exception as e:
            log.info(e)

        chat = {
            "chat_id": str(uuid.uuid1()),
            "timestamp": datetime.utcnow().isoformat(),
            "message": body.get("detail", {}).get("message", ""),
            "conversationId": body.get("detail", {}).get("conversationId", ""),
            "userId": body.get("detail", {}).get("userId", ""),
            "avatar": body.get("detail", {}).get("avatar", "")
        }
        dynamodb.Table('chat').put_item(
            Item=chat
        )

        del chat['chat_id']
        del chat['timestamp']
        chat['type'] = 'message'

    elif body.get("detail", {}).get("type", "") == "typing":
        chat = {
            "type": "typing",
            "conversationId": body.get("detail", {}).get("conversationId", ""),
            "userId": body.get("detail", {}).get("userId", ""),
            "isTyping": body.get("detail", {}).get("isTyping", ""),
            "content": body.get("detail", {}).get("isTyping", "")
        }

    table = dynamodb.Table("chat_connections")
    response = table.scan(ProjectionExpression="ConnectionId")
    items = response.get("Items", [])
    connections = [x["ConnectionId"] for x in items if "ConnectionId" in x]

    chat = {
        "detail": chat
    }

    for connectionID in connections:
        try:
            log.info(connectionID)
            _send_to_connection(connectionID, chat, event)
            log.info('sent to client')
        except Exception as e:
            log.info(e)

    return {
        "statusCode": 200,
        "body": "Message sent to connections"
    }


def _send_to_connection(connection_id, data, event):
    endpoint = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]

    gatewayapi = boto3.client("apigatewaymanagementapi",
                              endpoint_url=endpoint)
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
                                         Data=json.dumps(data).encode('utf-8'))


def call_google_recaptcha(input_token):
    secret = "6Ledii4dAAAAAKvn9t-Y1RvfeR7nmNAnZGejK1P_"
    conn = http.client.HTTPSConnection('www.google.com')
    conn.request('POST', f'/recaptcha/api/siteverify?secret={secret}&response={input_token}')
    response = conn.getresponse()
    json_response = json.loads(response.read())
    log.debug(f'recaptcha response: {json_response}')

    return json_response


def verifyPubKey(publicAddress, signature, hash_message):
    subTable = dynamodb.Table('sub')
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
        if 'Item' in subResponse:
            log.debug("found existing publicAddress")
            userId = subResponse['Item']['userId']
            log.info(f'userId: {userId}')

        else:
            userId = str(uuid.uuid1())
            subTable.put_item(
                Item={
                    "sub": publicAddress,
                    "userId": userId
                }
            )

        return True

    else:
        return False


def verifyNFTowner(public_address, contract_address):
    path = "/api/v1/assets?owner=" + public_address + "&asset_contract_address=" + contract_address + "&order_direction=desc&offset=0&limit=20"
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)
    print(len(open_sea_response['assets']))

    if len(open_sea_response['assets']) > 0:
        return True
    else:
        return False
