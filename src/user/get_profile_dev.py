import boto3
from datetime import datetime
import uuid
from botocore.config import Config
import json
from util import sudocoins_logger
import random
from ethereum.utils import ecrecover_to_pub, sha3
from eth_utils.hexadecimal import encode_hex, decode_hex, add_0x_prefix


log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    jsonInput = json.loads(event.get('body', '{}'))
    sub, email, facebook, signupMethod, ip, shareId, publicAddress, signature, hash_message = parseJson(jsonInput)

    global profile

    if publicAddress != "":
        try:
            profile = loadProfileByMetaAddress(publicAddress, 'MetaMask', signature, hash_message, context)
            log.debug(f'profile: {profile}')
        except Exception as e:
            log.exception(e)
            profile = {}
    else:
        try:
            if sub != "":
                profile = loadProfile(sub, email, facebook, signupMethod, context, ip, shareId)
                log.debug(f'profile: {profile}')
            else:
                profile = {}
        except Exception as e:
            log.exception(e)
            profile = {}
    try:
        config = getConfig()
        rate = getRate(config)

    except Exception as e:
        log.exception(e)
        rate = '1'

    log.debug(f'profile: {profile}')

    return {
        "profile": profile,
        "rate": str(rate),
        "sudoRate": str(1000)
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

def loadProfile(sub, email, facebook, signupMethod, context, ip, shareId):
    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': sub})
    log.info(f'subResponse: {subResponse}')
    log.info(f'email: {email}')

    if 'Item' in subResponse:
        log.debug("found userId matching sub")
        userId = subResponse['Item']['userId']
        log.info(f'userId: {userId}')
        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId,"
                                 "gravatarEmail, facebookUrl, history, "
                                 "verificationState, fraud_score, sudocoins,"
                                 "user_name, twitter_handle"
        )
        log.info(f'profileObject: {profileObject}')

        if 'Item' not in profileObject:
            profile = createProfile(email, profileTable, userId, facebook, signupMethod, context, shareId)
            return profile

        if 'verificationState' not in profileObject['Item']:
            profileObject['Item']['verificationState'] = 'None'
            profileTable.update_item(
                Key={
                    "userId": userId
                },
                UpdateExpression="set verificationState=:vs",
                ExpressionAttributeValues={
                    ":vs": 'None'
                },
                ReturnValues="ALL_NEW"
            )

        if 'facebookUrl' in profileObject['Item']:
            if facebook == profileObject['Item']['facebookUrl']:
                pass
            else:
                profileObject = profileTable.update_item(
                    Key={
                        "userId": userId
                    },
                    UpdateExpression="set facebookUrl=:fb",
                    ExpressionAttributeValues={
                        ":fb": facebook
                    },
                    ReturnValues="ALL_NEW"
                )

                if 'sudocoins' not in profileObject['Attributes']:
                    profileObject['Attributes']['sudocoins'] = '0'

                return profileObject['Attributes']

        return profileObject['Item']

    elif email != "":
        log.info("sub not found in sub table: seeing if user email matches any existing userId")
        profileQuery = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            },
            ProjectionExpression="active , email, signupDate, userId, "
                                 "gravatarEmail, facebookUrl, history,"
                                 "verificationState, fraud_score, sudocoins,"
                                 "user_name, twitter_handle"
        )
        log.info(f'profileQuery: {profileQuery}')
        if profileQuery['Count'] > 0:
            log.info("found email in database")
            userId = profileQuery['Items'][0]['userId']
            subTable.put_item(
                Item={
                    "sub": sub,
                    "userId": userId
                }
            )


            if 'verificationState' not in profileQuery['Items'][0]:
                profileQuery['Items'][0]['verificationState'] = "None"
                profileTable.update_item(
                    Key={
                        "userId": userId
                    },
                    UpdateExpression="set verificationState=:vs",
                    ExpressionAttributeValues={
                        ":vs": 'None'
                    },
                    ReturnValues="ALL_NEW"
                )
            if 'facebookUrl' in profileQuery['Items']:
                if facebook == profileQuery['Items']['facebookUrl']:
                    pass
                else:
                    profileResponse = profileTable.update_item(
                        Key={
                            "userId": userId
                        },
                        UpdateExpression="set facebookUrl=:fb",
                        ExpressionAttributeValues={
                            ":fb": facebook
                        },
                        ReturnValues="ALL_NEW"
                    )

                    return profileResponse['Attributes']

            return profileQuery['Items'][0]

    log.info("no sub or email found in database. New user.")
    userId = str(uuid.uuid1())

    if email == "":
        log.info("completely new user with no email in cognito")
        email = userId + "@sudocoins.com"

        subTable.put_item(
            Item={
                "sub": sub,
                "userId": userId
            }
        )
    profile = createProfile(email, profileTable, userId, facebook, signupMethod, context, shareId)

    return profile


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
    queue = sqs.get_queue_by_name(QueueName='affiliates.fifo')
    queue.send_message(MessageBody=json.dumps(profile), MessageGroupId='shareId')
    log.info("affiliate added")

    profile["new_user"] = True

    return profile


def parseJson(jsonInput):

    if 'sub' in jsonInput:
        sub = jsonInput['sub']
    else:
        sub = ""
    if 'email' in jsonInput:
        email = jsonInput['email']
    else:
        email = ""
    if 'facebookUrl' in jsonInput:
        facebook = jsonInput['facebookUrl']
    else:
        facebook = ""
    if 'signupMethod' in jsonInput:
        signupMethod = jsonInput['signupMethod']
    else:
        signupMethod = ""
    if 'ip' in jsonInput:
        ip = jsonInput['ip']
    else:
        ip = ""
    if 'shareId' in jsonInput:
        shareId = jsonInput['shareId']
    else:
        shareId = 'organic'
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


    return sub, email, facebook, signupMethod, ip, shareId, publicAddress, signature, hash_message

