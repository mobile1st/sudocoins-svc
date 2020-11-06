import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
from boto3.dynamodb.conditions import Key
import uuid


def lambda_handler(event, context):
    """
    Loads profile and userId or registers user if new. Returns survey tiles.
    :param event: sub and email from Cognito.
    :param context:
    :return: JSON object for the front end that contains profile and surveys
    """
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    sub = event['sub']
    if 'email' in event:
        email = event['email']

    try:
        profileResp = loadProfile(sub, email)
        print("load profile function complete")

    except Exception as e:
        print(e)

    try:
        if profileResp["currency"] == "" or "usd":
            rate = .01
            print("rate loaded in memory")
        else:
            rate = getRates(profileResp["currency"])
            print("rate loaded from db")

    except Exception as e:
        print(e)
        rate = .01
        profileResp["currency"] = 'usd'

    try:
        surveyTiles = getSurveyObject(profileResp['userId'], rate)
        print("survey list loaded")
        if surveyTiles is None:
            data = {'profile': profileResp, 'survey_tile': "Error fetching survey tile"}

            return {
                'statusCode': 400,
                'body': data
            }

    except Exception as e:
        data = {"profile": profileResp,"survey_tile": "Error fetching survey tile"}

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


def loadProfile(sub, email):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    profileTableName = os.environ["PROFILE_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table(profileTableName)
    subTable = dynamodb.Table('sub')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId, currency, gravatarEmail"
        )

        return profileObject['Item']

    else:

        profileQuery = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            },
            ProjectionExpression="active , email, signupDate, userId, currency, gravatarEmail"
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

        else:
            created = datetime.utcnow().isoformat()
            userId = str(uuid.uuid1())

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
                "currency": "",
                "gravatarEmail": email
            }

            profileResponse = profileTable.put_item(
                Item=profileObject
            )

            return profileObject


def getSurveyObject(userId, rate):
    """Fetches a list of open surveys for the user
    Arguments: userId
    Returns: list of survey urls and incentives
    """
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
    url = "https://www.sudocoins.com/prod/SudoCoinsTakeSurvey?"

    try:
        response = configTable.get_item(Key={'configKey': configKey})

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
                "incentive": float(i["defaultCpi"]) * rate,
                "url": url + "buyerName=" + i["name"] + "&userId=" + userId
            }
            surveyTiles.append(buyer)

        return surveyTiles


def getRates(currency):
    ratesTableName = os.environ["RATES_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ratesTable = dynamodb.Table(ratesTableName)

    ratesResponse = ratesTable.get_item(Key={'currency': currency})
    rate = ratesResponse['Item']["sudo"]

    return float(rate)
