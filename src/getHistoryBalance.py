import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    sub = event['sub']

    try:
        profileResp = loadProfile(sub)
        print("load profile function complete")
        print(profileResp)

    except Exception as e:
        print(e)
        return {
            "history": {},
            "balance": ""
        }

    if profileResp is None:

        return {
            "history": {},
            "balance": ""
        }
    else:
        try:
            if profileResp["currency"] == "" or "usd":
                rate = .01
                profileResp["currency"] = 'usd'
                print("rate loaded in memory")
            else:
                rate = getRates(profileResp["currency"])
                print("rate loaded from db")
        except Exception as e:
            print(e)
            rate = .01
            profileResp["currency"] = 'usd'
            print(profileResp)

        try:
            historyStatus, history = loadHistory(profileResp["userId"], rate, profileResp["currency"])
            print("history loaded")
        except Exception as e:
            print(e)
            history = {}

        try:
            balance = getBalance(history, profileResp["currency"])
            print("balance loaded")
        except Exception as e:
            print(e)
            balance = ""

        return {
            "balance": balance,
            "history": history
        }


def loadProfile(sub):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    dynamodb = boto3.resource('dynamodb')
    profileTableName = os.environ["PROFILE_TABLE"]
    subTable = dynamodb.Table('sub')
    profileTable = dynamodb.Table(profileTableName)

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        profileObject = profileTable.get_item(
            Key={'userId': userId},
            ProjectionExpression="active , email, signupDate, userId, currency, gravatarEmail"
        )

        return profileObject['Item']

    else:

        return None


def getBalance(history, currency):
    """Iterates through the user's history and computes the user's balance
    Arguments: list of ledger records, user's preferred currency
    Returns: the user's balance.
    """
    debit = 0
    credit = 0

    for i in history:
        if 'type' in i.keys():
            if i["type"] == "Cash Out":

                credit += float(i["amount"])
            elif 'amount' in i.keys() and i['amount'] != "":

                debit += float(i["amount"])

    balance = debit - credit
    if balance <= 0:
        return str(0)
    else:
        if currency == "usd":
            return str(round(balance, 2))
        elif currency == 'btc':
            return str(round(balance, 8))


def getRates(currency):
    ratesTableName = os.environ["RATES_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ratesTable = dynamodb.Table(ratesTableName)

    ratesResponse = ratesTable.get_item(Key={'currency': currency})
    rate = ratesResponse['Item']["sudo"]

    return float(rate)


def loadHistory(userId, rate, currency):
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
            IndexName='sortedHistory',
            ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
            ProjectionExpression="transactionId, lastUpdate, #t, #s, amount")
        history = ledgerHistory["Items"]

        for i in history:
            if 'amount' in i:
                if currency == 'usd':
                    i['amount'] = round(((float(i['amount'])) * rate), 2)
                elif currency == 'btc':
                    i['amount'] = round(((float(i['amount'])) * rate), 8)

    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", userId, e.response['Error']['Message'])

        return 'error', {}

    else:
        return 'success', history
