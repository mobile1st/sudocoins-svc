import boto3
from datetime import datetime
import uuid
from botocore.config import Config
import json
from util import sudocoins_logger

# from configuration import Configuration

log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    jsonInput = json.loads(event.get('body', '{}'))

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
    global profile

    try:
        if sub != "":
            profile = loadProfile(sub, email, facebook, signupMethod, context, ip)
            userId = profile['userId']
            log.debug(f'profile: {profile} userId: {userId}')
        else:
            profile = {}
            userId = ""

    except Exception as e:
        log.exception(e)
        profile = {}
        userId = ""

    try:
        config = getConfig()
        rate, ethRate = getRate(config)
        tiles = getTiles(userId, config)

    except Exception as e:
        log.exception(e)
        rate = '1'
        ethRate = '1'
        tiles = []

    log.debug(f'profile: {profile}')

    return {
        "profile": profile,
        "tiles": tiles,
        "rate": str(rate),
        "ethRate": str(ethRate),
        "sudoRate": str(1000)
    }


def loadProfile(sub, email, facebook, signupMethod, context, ip):
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
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance,"
                                 "verificationState, signupMethod, fraud_score, sudocoins"
        )
        log.info(f'profileObject: {profileObject}')
        if 'history' not in profileObject['Item']:
            profileObject['Item']['history'] = []

        if 'balance' not in profileObject['Item']:
            profileObject['Item']['balance'] = '0.00'

        if 'sudocoins' not in profileObject['Item']:
            profileObject['Item']['sudocoins'] = '0'

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

        if 'signupMethod' not in profileObject['Item']:
            profileObject['Item']['signupMethod'] = signupMethod
            profileTable.update_item(
                Key={
                    "userId": userId
                },
                UpdateExpression="set signupMethod=:sm",
                ExpressionAttributeValues={
                    ":sm": signupMethod
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

                if 'history' not in profileObject['Attributes']:
                    profileObject['Attributes']['history'] = []
                if 'balance' not in profileObject['Attributes']:
                    profileObject['Attributes']['balance'] = '0.00'
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
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance,"
                                 "verificationState, signupMethod, fraud_score, sudocoins"
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

            if 'history' not in profileQuery['Items'][0]:
                profileQuery['Items'][0]['history'] = []
            if 'balance' not in profileQuery['Items'][0]:
                profileQuery['Items'][0]['balance'] = "0.00"
            if 'sudocoins' not in profileQuery['Items'][0]:
                profileQuery['Items'][0]['sudocoins'] = "0"
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

                    if 'history' not in profileResponse['Attributes']:
                        profileResponse['Attributes']['history'] = []
                    if 'balance' not in profileResponse['Attributes']:
                        profileResponse['Attributes']['balance'] = "0.00"

                    return profileResponse['Attributes']

            return profileQuery['Items'][0]

    log.info("no sub or email found in database. New user.")
    created = datetime.utcnow().isoformat()
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
        "signupMethod": signupMethod
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
    log.debug("profile added to sns")

    profile["new_user"] = True

    return profile


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config


def getRate(config):
    rate = str(config['rate'])
    ethRate = str(config['ethRate'])

    return rate, ethRate


def getTiles(userId, config):
    try:
        buyerObject = []
        for i in config['configValue']['publicBuyers']:
            buyerObject.append(config['configValue']["buyers"][i])

        tiles = []
        for i in buyerObject:
            if i['type'] == "survey":
                buyer = {
                    "name": i["name"],
                    "type": i['type'],
                    "title": i["title"]
                }

                if userId == "":
                    url = i["urlGuest"]
                    buyer["url"] = url + "buyerName=" + buyer['name']

                else:
                    url = i['urlAuth']
                    buyer["url"] = url + "userId=" + userId + "&buyerName=" + buyer['name']

            elif i['type'] == 'giftCard':
                buyer = {
                    "name": i["name"],
                    "type": i['type'],
                    "title": i["tileTitle"],
                    "title2": i['productTitle'],
                    "description": i["description"],
                    "amounts": i['amounts'],
                    "cashBack": i['cashBack'],
                    "currencies": config['currencies']
                }

                if userId == "":
                    url = i["urlGuest"]
                    buyer["url"] = url

                else:
                    url = i['urlAuth']
                    buyer["url"] = url

            tiles.append(buyer)

        return tiles

    except Exception:
        log.exception('Could not get tiles')
