import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid
from decimal import Decimal


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
        rate = ".01"
    else:
        rate = jsonInput["rate"]

    sudoAmount = convertAmount(jsonInput['amount'], rate, jsonInput['type'])

    payout = {
        "paymentId": transactionId,
        "userId": userId,
        "amount": sudoAmount,
        "lastUpdate": lastUpdate,
        "type": jsonInput["type"],
        "address": jsonInput["address"],
        "Status": "Pending",
        "usdBtcRate": rate,
        "userInput": jsonInput["amount"]
    }
    # withdraw record added to ledger table
    withdraw = {
        "userId": userId,
        "amount": sudoAmount,
        "lastUpdate": lastUpdate,
        "type": "Cash Out",
        "status": "Pending",
        "transactionId": transactionId,
        "usdBtcRate": rate,
        "userInput": jsonInput["amount"]
    }
    payoutResponse = payoutTable.put_item(
        Item=payout
    )
    ledgerResponse = ledgerTable.put_item(
        Item=withdraw
    )
    return {
        'statusCode': 200,
        'body': json.dumps('Cash out received')
    }


def loadProfile(sub):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        return userId

    else:

        return None


def convertAmount(amount, rate, type):
    if type == "Bitcoin":
        sudoAmount = (Decimal(amount) / (Decimal(rate) / Decimal(100))).quantize(1)
        print(sudoAmount)
        return sudoAmount

    else:
        sudoAmount = (Decimal(amount) * 100).quantize(1)
        print(sudoAmount)
        return sudoAmount