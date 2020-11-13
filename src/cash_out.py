import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid


def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table("Ledger")
    payoutTable = dynamodb.Table("Payouts")

    jsonInput = event
    sub = jsonInput['sub']
    userId = loadProfile(sub)

    lastUpdate = datetime.utcnow().isoformat()
    transactionId = str(uuid.uuid1())

    payout = {
        "paymentId": transactionId,
        "userId": userId,
        "amount": jsonInput["amount"],
        "lastUpdate": lastUpdate,
        "type": jsonInput["type"],
        "address": jsonInput["address"],
        "Status": "Pending"
    }
    # withdraw record added to ledger table
    withdraw = {
        "userId": userId,
        "amount": jsonInput["amount"],
        "lastUpdate": lastUpdate,
        "type": "Cash Out",
        "status": "Pending",
        "transactionId": transactionId
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
    profileTable = dynamodb.Table('Profile')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        return userId

    else:

        return None