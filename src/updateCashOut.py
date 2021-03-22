import boto3
from datetime import datetime
import history


def lambda_handler(event, context):

    try:
        for i in event["records"]:
            userId = i['userId']
            transactionId = i['paymentId']
            print(userId)
            print(transactionId)

            updateCashOut(userId, transactionId)

        return {
            'body': "Success"
        }

    except Exception as e:
        print(e)
        return {
            'body': "Fail"
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