import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def loadProfile(user_id):
    profileTableName = os.environ["PROFILE_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table(profileTableName)
    try:
        response = profileTable.query(
            KeyConditionExpression=Key("UserId").eq(user_id),
        )
    except ClientError as e:
        print("Failed to query profile for userId=%s error=%s", user_id, e.response['Error']['Message'])
        return {
            'statusCode': 400,
            'profile': 'Invalid user_id'
        }
    else:
        if "currency" in response['Items'][0].keys():
            currency = response['Items'][0]["currency"]
        else:
            currency = "USD"
        if "lang" in response['Items'][0].keys():
            lang = response['Items'][0]["lang"]
        else:
            lang = "English"
        if "gravatarEmail" in response['Items'][0].keys():
            ge = response['Items'][0]["gravatarEmail"]
        else:
            ge = response['Items'][0]["Email"]

        profile_object = {
            "active": response['Items'][0]["Status"],
            "email": response['Items'][0]["Email"],
            "signupDate": response['Items'][0]["CreatedAt"],
            "UserID": response['Items'][0]["UserId"],
            "currency": currency,
            "lang": lang,
            "gravatarEmail": ge
        }
        return {
            'statusCode': 200,
            'profile': profile_object
        }


def loadHistory(user_id):
    ledgerTableName = os.environ["LEDGER_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(ledgerTableName)
    try:
        ledgerHistory = ledgerTable.query(
            KeyConditionExpression=Key("UserId").eq(user_id),
            ScanIndexForward=False)
        history = ledgerHistory["Items"]
    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", user_id, e.response['Error']['Message'])
        return 'error', {}
    else:
        return 'success', history


def getBalance(history):
    debit = 0
    credit = 0
    for i in history:
        if i["Type"] == "Cash Out":
            credit += float(i["Amount"])
        elif 'Amount' in i.keys() and i['Amount'] != "":
            debit += float(i["Amount"])
    balance = debit - credit
    if balance <= 0:
        return str(0)
    else:
        return str(debit)


def getSurveyObject(userId):
    configTableName = os.environ["CONFIG_TABLE"]
    configKey = "TakeSurveyPage"
    dynamodb = boto3.resource('dynamodb')
    configTable = dynamodb.Table(configTableName)
    URL = "https://cesyiqf0x6.execute-api.us-west-2.amazonaws.com/prod/SudoCoinsTakeSurvey?ip=108.50.251.254"
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
                "incentive": i["defaultCPI"],
                "URL": URL + "&buyer_name=" + i["name"] + "&user_id=" + userId
            }
            surveyTiles.append(buyer)
        return surveyTiles


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    jsonInput = event
    profileResp = loadProfile(jsonInput["user_id"])
    if profileResp["statusCode"] != 200:
        return {
            'statusCode': 400,
            'body': {
                "code": 1,
                "error": 'Invalid account'
            }
        }
    historyStatus, history = loadHistory(jsonInput["user_id"])

    profileResp["profile"]["balance"] = getBalance(history)

    surveyTile = getSurveyObject(jsonInput["user_id"])
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
    return {
        'statusCode': 200,
        'body': {
            "profile": profileResp["profile"],
            "history": history,
            "survey": surveyTile
        }
    }
