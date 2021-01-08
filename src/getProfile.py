import boto3
from datetime import datetime
import uuid

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

        rate, ethRate = getRate(config)

        print("about to get tiles from config")
        tiles = getTiles(userId, config)
        print("tiles loaded")

    except Exception as e:
        rate = '1'
        ethRate = '1'
        tiles = []
        print('failed to load tiles')

    print("about to return the entire response")

    return {
        'statusCode': 200,
        'body': {
            "profile": profile,
            "tiles": tiles,
            "rate": str(rate),
            "ethRate": str(ethRate)
        }
    }


def loadProfile(dynamodb, sub, email, facebook):
    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')
    subResponse = subTable.get_item(Key={'sub': sub})
    print(subResponse)
    print(email)

    if 'Item' in subResponse:
        print("found userId matching sub")
        userId = subResponse['Item']['userId']
        print(userId)
        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance"
        )
        print(profileObject)
        if 'history' not in profileObject['Item']:
            profileObject['Item']['history'] = []

        if 'balance' not in profileObject['Item']:
            profileObject['Item']['balance'] = '0.00'

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

                return profileObject['Attributes']

        return profileObject['Item']

    elif email != "":
        print("no sub but email found")
        profileQuery = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            },
            ProjectionExpression="active , email, signupDate, userId, currency, "
                                 "gravatarEmail, facebookUrl, consent, history, balance"
        )
        print(profileQuery)
        if profileQuery['Count'] > 0:
            print("found email in database")
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

    print("no sub or email found in database. New user.")
    created = datetime.utcnow().isoformat()
    userId = str(uuid.uuid1())

    if email == "":
        print("completely new user with no email in cognito")
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
    print(profile)
    profileTable.put_item(
        Item=profile
    )
    print("profile submitted")
    return profile


def getConfig(dynamodb):
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

    except Exception as e:
        print(e)


