from datetime import datetime
import boto3
import uuid
from decimal import Decimal
from history import History


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table("Ledger")
    payoutTable = dynamodb.Table("Payouts")
    profileTable = dynamodb.Table("Profile")

    loadHistory = History(dynamodb)

    jsonInput = event
    sub = jsonInput['sub']
    userId = loadProfile(sub)

    lastUpdate = datetime.utcnow().isoformat()
    transactionId = str(uuid.uuid1())

    if "rate" not in jsonInput:
        rate = "1"
    else:
        rate = str(jsonInput["rate"])

    payoutAmount = convertAmount(jsonInput['amount'], jsonInput['type'])

    payout = {
        "paymentId": transactionId,
        "userId": userId,
        "amount": payoutAmount,
        "lastUpdate": lastUpdate,
        "type": jsonInput["type"],
        "address": jsonInput["address"],
        "status": "Pending",
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

    client = boto3.client("sns")
    client.publish(
        PhoneNumber="+16282265769",
        Message="Cash Out submitted"
    )

    loadHistory.updateProfile(userId)

    profileObject = profileTable.get_item(
        Key={'userId': userId},
        ProjectionExpression="history, balance"
    )

    if 'history' not in profileObject['Item']:
        profileObject['Item']['history'] = []

    if 'balance' not in profileObject['Item']:
        profileObject['Item']['balance'] = '0.00'


    return {
        'statusCode': 200,
        'history': profileObject['Item']['history'],
        'balance': profileObject['Item']['balance']
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


def convertAmount(amount, type):
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


