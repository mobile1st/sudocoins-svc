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

    lastUpdate = datetime.utcnow().isoformat()
    transactionId = str(uuid.uuid1())

    sub = event['sub']
    userId, verificationState = loadProfile(sub)

    payout = {
        "paymentId": transactionId,
        "userId": userId,
        "amount": event['amount'],
        "lastUpdate": lastUpdate,
        "type": event["type"],
        "status": "Pending",
        "usdBtcRate": event['rate'],
        "sudoRate": event['sudoRate'],
        "verificationState": verificationState,
        "address": event["address"]
    }
    withdraw = {
        "userId": userId,
        "amount": event['amount'],
        "lastUpdate": lastUpdate,
        "type": "Cash Out",
        "status": "Pending",
        "transactionId": transactionId,
        "usdBtcRate": event['rate'],
        "verificationState": verificationState
    }

    payoutTable.put_item(
        Item=payout
    )
    ledgerTable.put_item(
        Item=withdraw
    )
    profileTable.update_item(
        Key={'userId': userId},
        UpdateExpression="SET sudocoins = :val",
        ExpressionAttributeValues={
            ':val': 0
        },
        ReturnValues="UPDATED_NEW"
    )

    client = boto3.client("sns")
    client.publish(
        PhoneNumber="+16282265769",
        Message="Cash Out submitted"
    )

    loadHistory = History(dynamodb)
    loadHistory.updateProfile(userId)

    profileObject = profileTable.get_item(
        Key={'userId': userId},
        ProjectionExpression="history"
    )

    if 'history' not in profileObject['Item']:
        profileObject['Item']['history'] = []

    return {
        'statusCode': 200,
        'sudocoins': 0,
        'history': profileObject['Item']['history'],
        'balance': "0.00"
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
        profileResponse = profileTable.get_item(Key={'userId': userId})

        if 'verificationState' in profileResponse['Item']:
            verificationState = profileResponse['Item']['verificationState']
        else:
            verificationState = 'None'

        return userId, verificationState

    else:

        return None
