import boto3
from datetime import datetime
import uuid
from decimal import *
import json


# from configuration import Configuration


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    # . jsonInput = json.loads(event['body'])
    jsonInput = event
    print(event)

    # begin testing configuration access
    # config = Configuration(dynamodb)
    # try:
    #     print('config for test buyer=%s', config.buyer('test'))
    #     print('config publicBuyers=%s', config.public_buyers())
    # except Exception as e:
    #     print('Config read exception: ', e)
    # end testing configuration access

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

    global profile

    try:
        if sub != "":
            print("about to load profile")
            profile = loadProfile(dynamodb, sub, email, facebook)
            print("profile loaded")
            userId = profile['userId']
        else:
            profile = {}
            userId = ""

    except Exception as e:
        print("issue loading profile")
        # profile.update(history = [])
        # profile.update(balance = "")
        profile = {}
        userId = ""
        print(e)

    try:
        print("about to get config")
        config = getConfig(dynamodb)
        print("config loaded")

        rate = getRate(config)

        print("about to get tiles from config")
        tiles = getTiles(userId, config)
        print("tiles loaded")

    except Exception as e:
        rate = '1'
        tiles = []
        print('failed to load tiles')

    print("about to return the entire response")

    return {
        'statusCode': 200,
        'body': {
            "profile": profile,
            "tiles": tiles,
            "rate": str(rate)
        }
    }


def loadProfile(dynamodb, sub, email, facebook):
    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': sub})
    print(subResponse)
    print(email)

    if 'Item' in subResponse:
        print("founder userId matching sub")
        userId = subResponse['Item']['userId']
        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance"
        )

        if 'history' not in profileObject['Item']:
            profileObject['Item']['history'] = []

        if 'balance' not in profileObject['Item']:
            profileObject['Item']['balance'] = '0.00'

        return profileObject['Item']

    elif email != "":

        profileQuery = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            },
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance"
        )

        if profileQuery['Count'] > 0:
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

            return profileQuery['Items'][0]
    else:
        created = datetime.utcnow().isoformat()
        userId = str(uuid.uuid1())

        if email == "":
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
            "balance": "0.00"
        }

        profileTable.put_item(
            Item=profile
        )

        return profile


def getConfig(dynamodb):
    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config


def getRate(config):
    rate = str(config['rate'])

    return rate


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
                    "title": i["tileTitle"]
                }

                if userId == "":
                    url = i["urlGuest"]
                    buyer["url"] = url

                else:
                    url = i['urlAuth']
                    buyer["url"] = url

            tiles.append(buyer)

        return tiles

    except Exception as e:
        print(e)


