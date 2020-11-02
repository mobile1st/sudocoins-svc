import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import requests


def loadProfile(userId):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    profileTableName = os.environ["PROFILE_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table(profileTableName)

    try:
        response = profileTable.query(
            KeyConditionExpression=Key("userId").eq(userId),
        )

    except ClientError as e:
        print("Failed to query profile for userId=%s error=%s", userId, e.response['Error']['Message'])

        return {
            'statusCode': 400,
            'profile': 'Invalid user_id'
        }

    else:
        if "currency" in response['Items'][0].keys():
            currency = response['Items'][0]["currency"]
        else:
            currency = "usd"

        if "lang" in response['Items'][0].keys():
            lang = response['Items'][0]["lang"]
        else:
            lang = "en"

        if "gravatarEmail" in response['Items'][0].keys():
            ge = response['Items'][0]["gravatarEmail"]
        else:
            ge = response['Items'][0]["email"]

        profile_object = {
            "active": response['Items'][0]["status"],
            "email": response['Items'][0]["email"],
            "signupDate": response['Items'][0]["createdAt"],
            "userId": response['Items'][0]["userId"],
            "currency": currency,
            "lang": lang,
            "gravatarEmail": ge
        }

        return {
            'statusCode': 200,
            'profile': profile_object
        }


def loadHistory(userId):
    """Fetches the user history from the Ledger table.
    Arguments: userId.
    Returns: a list of of objects, each representing a user's transaction.
    """
    ledgerTableName = os.environ["LEDGER_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(ledgerTableName)

    try:
        ledgerHistory = ledgerTable.query(
            KeyConditionExpression=Key("userId").eq(userId),
            ScanIndexForward=False,
            ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
            ProjectionExpression="transactionId, lastUpdate, #t, #s, amount")
        history = ledgerHistory["Items"]

    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", userId, e.response['Error']['Message'])

        return 'error', {}

    else:
        return 'success', history


def getBalance(history, currency):
    """Iterates through the user's history and computes the user's balance
    Arguments: list of ledger records, user's preferred currency
    Returns: the user's balance.
    """
    debit = 0
    credit = 0

    for i in history:
        if i["type"] == "Cash Out":
            credit += float(i["amount"])
        elif 'amount' in i.keys() and i['amount'] != "":
            debit += float(i["amount"])

    balance = debit - credit
    print(balance)
    if balance <= 0:
        return str(0)
    else:
        if currency == "btc":
            url = 'https://blockchain.info/tobtc?'
            params = {
                "currency":"USD",
                "value":str(balance*100)
            }
            response = requests.get(url, params=params)
            btcBalance = response.content.decode("utf-8")
            return str(btcBalance)
        elif currency == "usd":
            return str(100*balance)
        else:
            return str(balance)


def getSurveyObject(userId):
    """Fetches a list of open surveys for the user
    Arguments: userId
    Returns: list of survey urls and incentives
    """
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
    url = "https://cesyiqf0x6.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsTakeSurvey?"

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
                "incentive": i["defaultCpi"],
                "url": url + "buyerName=" + i["name"] + "&userId=" + userId
            }
            surveyTiles.append(buyer)

        return surveyTiles


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    jsonInput = event

    try:
        profileResp = loadProfile(jsonInput["user_id"])
        print("profile loaded")
    except Exception as e:
        print(e)

        return {
            'statusCode': 400,
            'body': {
                "code": 1,
                "error": 'Invalid account'
            }
        }

    try:
        historyStatus, history = loadHistory(jsonInput["user_id"])
        print("history loaded")
    except Exception as e:
        print(e)
        history = {}

    try:
        profileResp["profile"]["balance"] = getBalance(history, profileResp["profile"]["currency"])
        print("balance loaded")
    except Exception as e:
        print(e)
        profileResp["profile"]["balance"] = ""

    try:
        surveyTile = getSurveyObject(jsonInput["user_id"])
        print("survey list loaded")
        if surveyTile is None:
            data = {
                "code": 3,
                "profile": profileResp["profile"],
                "history": history,
                "survey_tile": "Error fetching survey tile"
            }

            return {
                'statusCode': 400,
                'body': data
            }

    except Exception as e:
        data = {
            "code": 3,
            "profile": profileResp["profile"],
            "history": history,
            "survey_tile": "Error fetching survey tile"
        }

        return {
            'statusCode': 400,
            'body': data
        }
    print("about to return")
    return {
        'statusCode': 200,
        'body': {
            "profile": profileResp["profile"],
            "history": history,
            "survey": surveyTile
        }
    }
