import boto3
from datetime import datetime
import history


def lambda_handler(event, context):
    success = []
    fail = []

    for i in event["records"]:
        try:
            userId = i['userId']
            transactionId = i['transactionId']
            updateCashOut(userId, transactionId)
            success.append(transactionId)

        except Exception as e:
            print(e)
            fail.append(i['transactionId'])

    return {
        'success': success,
        'fail': fail
    }


def updateCashOut(userId, transactionId):
    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table('Ledger')
    payoutTable = dynamodb.Table('Payouts')

    now = str(datetime.utcnow().isoformat())

    payoutTable.update_item(
        Key={
            "paymentId": transactionId
        },
        UpdateExpression="set #s=:s, lastUpdate=:lu",
        ExpressionAttributeValues={
            ":s": "Complete",
            ":lu": now

        },
        ExpressionAttributeNames={'#s': 'status'},
        ReturnValues="ALL_NEW"
    )

    ledgerTable.update_item(
        Key={
            "userId": userId,
            "transactionId": transactionId
        },
        UpdateExpression="set #s=:s, lastUpdate=:lu",
        ExpressionAttributeValues={
            ":s": "Complete",
            ":lu": now

        },
        ExpressionAttributeNames={'#s': 'status'},
        ReturnValues="ALL_NEW"
    )

    profile = history.History(dynamodb)
    profile.updateProfile(userId)