import os
import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from exchange_rates import ExchangeRates


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    sub = event['sub']

    dynamodb = boto3.resource('dynamodb')
    exchange = ExchangeRates(dynamodb)

    try:
        profileResp = loadProfile(sub)
        print("load profile function complete")



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
            print(profileResp)
            if profileResp["currency"] == "":
                rate = Decimal('.01')
                precision = Decimal('1.00')
                profileResp["currency"] = 'usd'
                print("rate loaded in memory")
            else:
                print("try")
                rate, precision = exchange.get_rate(profileResp["currency"])
                print("rate loaded from db")
        except Exception as e:
            print(e)
            rate = Decimal('.01')
            precision = Decimal('1.00')
            profileResp["currency"] = 'usd'
            print(profileResp)

        try:
            historyStatus, history = loadHistory(profileResp["userId"], rate, precision, profileResp["currency"])
            print("history loaded")
        except Exception as e:
            print(e)
            history = {}

        try:
            balance = getBalance(history, profileResp["currency"], precision)
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


def getBalance(history, currency, precision):
    """Iterates through the user's history and computes the user's balance
    Arguments: list of ledger records, user's preferred currency
    Returns: the user's balance.
    """
    debit = 0
    credit = 0

    for i in history:
        if 'type' in i.keys():
            if i["type"] == "Cash Out":
                credit += Decimal(i["amount"])

            elif 'amount' in i.keys() and i['amount'] != "":
                debit += Decimal(i["amount"])

    balance = debit - credit
    if balance <= 0:
        return Decimal(0)
    else:
        return balance.quantize(Decimal(10) ** -precision)


def loadHistory(userId, rate, precision, currency):
    """Fetches the user history from the Ledger table.
    Arguments: userId.
    Returns: a list of of objects, each representing a user's transaction.
    """
    ledgerTableName = os.environ["LEDGER_TABLE"]
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table(ledgerTableName)
    try:
        print("here?")
        ledgerHistory = ledgerTable.query(
            KeyConditionExpression=Key("userId").eq(userId),
            ScanIndexForward=False,
            IndexName='sortedHistory',
            ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
            ProjectionExpression="transactionId, lastUpdate, #t, #s, amount")
        history = ledgerHistory["Items"]
        for i in history:
            print(i)
            if 'amount' in i :
                if i['amount'] == "":
                    i['amount'] = Decimal(0)
                else:
                    i['amount'] = ((Decimal(i['amount'])) * rate).quantize(
                        Decimal('10') ** ((-1) * int(precision)))


    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", userId, e.response['Error']['Message'])

        return 'error', {}

    else:
        return 'success', history
