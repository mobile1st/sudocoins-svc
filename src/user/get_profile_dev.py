import boto3
from datetime import datetime
import uuid
from botocore.config import Config
import json
from util import sudocoins_logger
import random
from ethereum.utils import ecrecover_to_pub, sha3
from eth_utils.hexadecimal import encode_hex, decode_hex, add_0x_prefix
import http.client

log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    #. publicAddress = event['public']
    jsonInput = json.loads(event.get('body', '{}'))
    signupMethod, publicAddress, signature, hash_message = parseJson(jsonInput)

    try:
        msg = {
            "public_address": publicAddress
        }

        sns_client.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:GetMetaMaskTopic',
            MessageStructure='string',
            Message=json.dumps(msg)
        )
        log.info(f"meta mask message added")

    except Exception as e:
        log.info(e)

    global profile

    if publicAddress != "":
        try:
            profile = loadProfileByMetaAddress(publicAddress, 'MetaMask', signature, hash_message, context)
            log.debug(f'profile: {profile}')
            arts = get_metamask_arts(publicAddress)
        except Exception as e:
            log.exception(e)
            profile = {}
            arts = []
    try:
        config = getConfig()

    except Exception as e:
        log.exception(e)

    log.debug(f'profile: {profile}')

    return {
        "profile": profile,
        "earn": config['earn'],
        "ethRate": config['ethRate'],
        "arts": arts
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def loadProfileByMetaAddress(publicAddress, signupMethod, signature, hash_message, context):
    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': publicAddress})
    log.info(f'subResponse: {subResponse}')
    log.info(f'signupMethod: {signupMethod}')

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
            log.debug("found userid matching publicAddress")
            userId = subResponse['Item']['userId']
            log.info(f'userId: {userId}')
            profileObject = profileTable.get_item(Key={'userId': userId})
            log.info(f'profileObject: {profileObject}')

            if 'Item' not in profileObject:
                email = userId + "@sudocoins.com"
                log.info(f'create profile:')
                profile = createProfile(email, profileTable, userId, '', signupMethod, context, '')
                return profile
            else:
                return profileObject['Item']
        log.debug("no sub or email found in database. New user.")
        userId = str(uuid.uuid1())

        log.debug("completely new user with no email in cognito")
        email = userId + "@sudocoins.com"

        subTable.put_item(
            Item={
                "sub": publicAddress,
                "userId": userId
            }
        )
        profile = createProfile(email, profileTable, userId, '', signupMethod, context, '')

        return profile
    else:
        return {
            'status': 404,
            'message': 'User not found.'
        }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config


def getRate(config):
    rate = str(config['rate'])

    return rate


def create_user_name(email, profileTable):
    un_index = email.find('@')
    un = email[:un_index]
    profileQuery = profileTable.query(
        IndexName='user_name-index',
        KeyConditionExpression='user_name = :user_name',
        ExpressionAttributeValues={
            ':user_name': un
        }
    )
    if profileQuery['Count'] > 0:
        appendix = str(random.randint(0, 1000))
        new_un = un + appendix

        return new_un
    return un


def createProfile(email, profileTable, userId, facebook, signupMethod, context, shareId):
    created = datetime.utcnow().isoformat()
    user_name = create_user_name(email, profileTable)
    profile = {
        "active": True,
        "email": email,
        "signupDate": created,
        "userId": userId,
        "currency": "usd",
        "gravatarEmail": email,
        "facebookUrl": facebook,
        "consent": "",
        "history": [],
        "balance": 0,
        "sudocoins": 0,
        "verificationState": None,
        "signupMethod": signupMethod,
        "user_name": user_name,
        "twitter_handle": None,
        "affiliate": shareId
    }

    log.info(f'profile: {profile}')
    profileTable.put_item(
        Item=profile
    )
    log.debug("profile submitted")

    sns_client.publish(
        TopicArn="arn:aws:sns:us-west-2:977566059069:transaction-event",
        MessageStructure='string',
        MessageAttributes={
            'source': {
                'DataType': 'String',
                'StringValue': 'PROFILE'
            }
        },
        Message=json.dumps({
            'userId': userId,
            'source': 'PROFILE',
            'status': 'CREATED',
            'awsRequestId': context.aws_request_id,
            'timestamp': created,
            'signUpMethod': signupMethod
        })
    )
    log.info("profile added to sns")

    sqs = boto3.resource('sqs')

    profile["new_user"] = True

    return profile


def parseJson(jsonInput):
    if 'signupMethod' in jsonInput:
        signupMethod = jsonInput['signupMethod']
    else:
        signupMethod = ""
    if 'publicAddress' in jsonInput:
        publicAddress = jsonInput['publicAddress']
    else:
        publicAddress = ""
    if 'signature' in jsonInput:
        signature = jsonInput['signature']
    else:
        signature = ""
    if 'hash_message' in jsonInput:
        hash_message = jsonInput['hash_message']
    else:
        hash_message = ''

    return signupMethod, publicAddress, signature, hash_message


def get_metamask_arts(public_address):
    path = "https://api.opensea.io/api/v1/assets?owner=" + public_address + "&limit=50&offset=" + "0"
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)['assets']

    arts = []
    arts = arts + open_sea_response

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

        msg = {
            "art_url": i.get("image_url", ""),
            "preview_url": i.get("image_preview_url", ""),
            "name": i.get("name", ""),
            "description": i.get("description", ""),
            "collection_address": i.get("asset_contract", {}).get("address", ""),
            "collection_name": i.get("collection", {}).get("name", ""),
            "contractId#tokenId": contract_token
        }

        if i['last_sale'] is not None:
            msg['last_sale_price'] = i.get('last_sale', {}).get('total_price')

        if i['sell_orders'] is not None and len(i['sell_orders']) > 0:
            msg['list_price'] = i.get('sell_orders')[0]['current_price']

        art_objects.append(msg)

    return art_objects


