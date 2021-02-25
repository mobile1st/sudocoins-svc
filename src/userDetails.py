import boto3
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    userId = event['userId']

    dynamodb = boto3.resource('dynamodb')
    ledgerTable = dynamodb.Table('Ledger')
    transactionTable = dynamodb.Table('Transactions')
    #  verification = dynamodb.Table('Verifications')

    ledgerObject = ledgerTable.query(
        KeyConditionExpression=Key("userId").eq(userId),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='byUserId',
        ProjectionExpression="userId, transactionId, amount, lastUpdate, payoutType, "
                             "#s, #t, usdBtcRate, userInput")

    transactionsObject = transactionTable.query(
        KeyConditionExpression=Key("userId").eq(userId),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='userId-started-index',
        ProjectionExpression="userId, transactionId, buyer, completed, started,"
                             "ip, payout, redirect, revenue, revShare, #s, "
                             "surveyCode, #t")

    details = {
        "ledger": ledgerObject['Items'],
        "transactions": transactionsObject['Items'],
        "verification": ""
    }

    return details
