import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
import uuid
from decimal import *
from exchange_rates import ExchangeRates
import json


def lambda_handler(event, context):
    """
    Loads profile and userId or registers user if new. Returns survey tiles.
    :param event: sub and email from Cognito.
    :param context:
    :return: JSON object for the front end that contains profile and surveys
    """
    print("event=%s userId=%", event, context.identity.cognito_identity_id)

    sub = event['sub']

    dynamodb = boto3.resource('dynamodb')
    exchange = ExchangeRates(dynamodb)

    if 'email' in event:
        email = event['email']
    else:
        email = ""

    if 'facebookUrl' in event:
        fbook = event['facebookUrl']
    else:
        fbook = ""

    profileResp = {}

    try:
        profileResp = loadProfile(sub, email, fbook)
        print("load profile function complete")

    except Exception as e:
        print(e)

    try:
        if profileResp["currency"] == "":
            rate = Decimal(.01)
            precision = 2
            print("rate loaded in memory")
        else:
            rate, precision = exchange.get_rate(profileResp["currency"])
            print("rate loaded from db")

    except Exception as e:
        print(e)
        rate = Decimal(.01)
        precision = 2
        profileResp["currency"] = 'usd'

    try:
        surveyTiles = getSurveyObject(profileResp['userId'], rate, precision)
        print("survey list loaded")
        if surveyTiles is None:
            data = {'profile': profileResp, 'survey_tile': "Error fetching survey tile"}

            return {
                'statusCode': 400,
                'body': data
            }

    except Exception as e:
        data = {"profile": profileResp, "survey_tile": "Error fetching survey tile"}

        return {
            'statusCode': 400,
            'body': data
        }

    print("about to return the entire response")
    return {
        'statusCode': 200,
        'body': {
            "profile": profileResp,
            "survey": surveyTiles
        }
    }


def loadProfile(sub, email, fbook):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    dynamodb = boto3.resource('dynamodb')
    '''
    AmazonDaxClient.resource(
        endpoint_url='dax.abcdef.us-east-1.amazon.com')
    '''
    profileTable = dynamodb.Table('Profile')
    subTable = dynamodb.Table('sub')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId, currency, gravatarEmail, facebookUrl"
        )

        return profileObject['Item']

    elif email != "":

        profileQuery = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            },
            ProjectionExpression="active , email, signupDate, userId, currency, gravatarEmail, facebookUrl"
        )

        if profileQuery['Count'] > 0:
            userId = profileQuery['Items'][0]['userId']
            subResponse = subTable.put_item(
                Item={
                    "sub": sub,
                    "userId": userId
                }
            )

            return profileQuery['Items'][0]

    created = datetime.utcnow().isoformat()
    userId = str(uuid.uuid1())

    if email == "":
        email = userId + "@sudocoins.com"

    subResponse = subTable.put_item(
        Item={
            "sub": sub,
            "userId": userId
        }
    )

    profileObject = {
        "active": True,
        "email": email,
        "signupDate": created,
        "userId": userId,
        "currency": "usd",
        "gravatarEmail": email,
        "facebookUrl": fbook
    }

    profileResponse = profileTable.put_item(
        Item=profileObject
    )

    return profileObject


def getSurveyObject(userId, rate, precision):
    """Fetches a list of open surveys for the user
    Arguments: userId
    Returns: list of survey urls and incentives
    """
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
    url = "https://cesyiqf0x6.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsTakeSurvey?"

    print("here we go")

    try:
        response = configTable.get_item(Key={'configKey': configKey})
        print("response")
    except ClientError as e:
        print("Failed to query config error=%s", e.response['Error']['Message'])
        return None

    else:
        configData = response['Item']
        buyerObject = []
        for i in configData['configValue']['publicBuyers']:
            buyerObject.append(configData['configValue']["buyer"][i])

        surveyTiles = []
        for i in buyerObject:
            buyer = {
                "name": i["name"],
                "iconLocation": i["iconLocation"],
                "incentive": str((Decimal(i["defaultCpi"]) * rate * Decimal(i['revShare'])).quantize(
                    Decimal('10') ** ((-1) * int(precision)))),
                "url": url + "buyerName=" + i["name"] + "&userId=" + userId
            }
            surveyTiles.append(buyer)

        return surveyTiles

