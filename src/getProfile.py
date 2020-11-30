import boto3
from datetime import datetime
import uuid
from decimal import *
from .configuration import Configuration


def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource('dynamodb')
    config = Configuration(dynamodb)

    # begin testing configuration access
    try:
        print('config for test buyer=%s', config.buyer('test'))
        print('config publicBuyers=%s', config.public_buyers())
    except Exception as e:
        print('Config read exception: %s', e)
    # end testing configuration access

    sub = event['sub']
    if 'email' in event:
        email = event['email']
    else:
        email = ""
    if 'facebookUrl' in event:
        facebook = event['facebookUrl']
    else:
        facebook = ""

    try:
        print("about to load profile")
        profile = loadProfile(dynamodb, sub, email, facebook)
        print("profile loaded")

    except Exception as e:
        print("issue loading profile")
        profile = []
        profile['history'] = []
        profile['balance'] = ""
        print(e)

    try:
        print("about to get config")
        config = getConfig(dynamodb)
        print("config loaded")
        rate = getRate(config)
        print("about to get surveys from config")
        surveys = getSurveys(profile['userId'], config)
        print("surveys loaded")

    except Exception as e:
        rate = '1'
        surveys = []
        print('failed to load surveys')

    print("about to return the entire response")
    return {
        'statusCode': 200,
        'body': {
            "profile": profile,
            "surveys": surveys,
            "rate": rate
        }
    }


def loadProfile(dynamodb, sub, email, facebook):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """

    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')

    subResponse = subTable.get_item(Key={'sub': sub})

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
            profileObject['Item']['balance'] = ""

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

            if 'history' not in profileObject['Items'][0]:
                profileObject['Items'][0]['history'] = []
            if 'balance' not in profileObject['Items'][0]:
                profileObject['Items'][0]['balance'] = ""

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
            "balance": str("0.00")
        }

        profileTable.put_item(
            Item=profile
        )

        return profile


def getConfig(dynamodb):
    configTable = dynamodb.Table('Config')
    configKey = "TakeSurveyPage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config


def getRate(config):
    rate = config['rate']
    return rate


def getSurveys(userId, config):
    rate = Decimal('.01')
    precision = 2

    url = "https://cesyiqf0x6.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsTakeSurvey?"

    buyerObject = []
    for i in config['configValue']['publicBuyers']:
        buyerObject.append(config['configValue']["buyer"][i])

    surveys = []
    for i in buyerObject:
        buyer = {
            "name": i["name"],
            "iconLocation": i["iconLocation"],
            "incentive": str((Decimal(i["defaultCpi"]) * rate * Decimal(i['revShare'])).quantize(
                Decimal('10') ** ((-1) * int(precision)))),
            "url": url + "buyerName=" + i["name"] + "&userId=" + userId
        }
        surveys.append(buyer)

    return surveys

