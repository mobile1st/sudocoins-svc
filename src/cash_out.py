from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
from decimal import Decimal
from boto3.dynamodb.conditions import Key


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table("Ledger")
    payoutTable = dynamodb.Table("Payouts")

    jsonInput = event
    sub = jsonInput['sub']
    userId = loadProfile(sub)

    lastUpdate = datetime.utcnow().isoformat()
    transactionId = str(uuid.uuid1())

    if "rate" not in jsonInput:
        rate = "1"
    else:
        rate = jsonInput["rate"]

    payoutAmount = convertAmount(jsonInput['amount'], rate, jsonInput['type'])

    payout = {
        "paymentId": transactionId,
        "userId": userId,
        "amount": payoutAmount,
        "lastUpdate": lastUpdate,
        "type": jsonInput["type"],
        "address": jsonInput["address"],
        "Status": "Pending",
        "usdBtcRate": rate,
        "userInput": jsonInput["amount"],
        "payoutType": jsonInput['type']
    }
    # withdraw record added to ledger table
    withdraw = {
        "userId": userId,
        "amount": payoutAmount,
        "lastUpdate": lastUpdate,
        "type": "Cash Out",
        "status": "Pending",
        "transactionId": transactionId,
        "usdBtcRate": rate,
        "userInput": jsonInput["amount"],
        "payoutType": jsonInput['type']
    }
    payoutResponse = payoutTable.put_item(
        Item=payout
    )
    print("payout submitted")
    ledgerResponse = ledgerTable.put_item(
        Item=withdraw
    )
    print("cash out submitted")

    try:
        historyStatus, history = loadHistory(userId)
        print("history loaded")

    except Exception as e:
        print(e)
        history = {}

    try:
        balance = getBalance(history)
        print("balance loaded")

    except Exception as e:
        print(e)
        balance = ""

    return {
        'statusCode': 200,
        'history': history,
        'balance': balance
    }


def loadProfile(sub):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')
    profileTable = dynamodb.Table('Profile')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        return userId

    else:

        return None


def convertAmount(amount, rate, type):
    '''
    if type == "Bitcoin":
        payoutAmount = (Decimal(amount) * (Decimal(rate)) * Decimal(100)).quantize(Decimal(10) ** (-8))
        print(payoutAmount)

        return str(payoutAmount)

    else:
        payoutAmount = str((Decimal(amount)*Decimal('100')))
    '''
    payoutAmount = str((Decimal(amount) * Decimal('100')))
    return str(payoutAmount)


def getBalance(history):
    """Iterates through the user's history and computes the user's balance
    Arguments: list of ledger records, user's preferred currency
    Returns: the user's balance.
    """
    precision = 2
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
        precision = 2
        balance = str(Decimal(0).quantize(Decimal(10) ** ((-1) * int(precision))))
    else:
        balance = str(balance.quantize(Decimal(10) ** ((-1) * int(precision))))

    return balance


def loadHistory(userId):
    """Fetches the user history from the Ledger table.
    Arguments: userId.
    Returns: a list of of objects, each representing a user's transaction.
    """
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table('Ledger')

    rate = Decimal('.01')
    precision = 2

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
                if i['amount'] == "":
                    i['amount'] = Decimal(0)
                else:
                    i['amount'] = str(((Decimal(i['amount'])) * rate).quantize(
                        Decimal('10') ** ((-1) * int(precision))))
            if 'lastUpdate' in i:
                utcTime = datetime.strptime(i['lastUpdate'], "%Y-%m-%dT%H:%M:%S.%f")
                epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                i['epochTime'] = epochTime



    except ClientError as e:
        print("Failed to query ledger for userId=%s error=%s", userId, e.response['Error']['Message'])

        return 'error', {}

    else:
        return 'success', history


