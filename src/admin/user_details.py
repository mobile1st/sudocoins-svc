import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    user_id = event['rawPath'].replace('/user/', '').replace('/details', '')

    ledger_table = dynamodb.Table('Ledger')
    transaction_table = dynamodb.Table('Transaction')
    #  verification = dynamodb.Table('Verification')

    ledger_row = ledger_table.query(
        KeyConditionExpression=Key("userId").eq(user_id),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='byUserId',
        ProjectionExpression="userId, transactionId, amount, lastUpdate, payoutType, "
                             "#s, #t, usdBtcRate, userInput")

    transactions_row = transaction_table.query(
        KeyConditionExpression=Key("userId").eq(user_id),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='userId-started-index',
        ProjectionExpression="userId, transactionId, buyer, completed, started,"
                             "ip, payout, redirect, revenue, revShare, #s, "
                             "surveyCode, #t")

    return {
        "ledger": ledger_row['Items'],
        "transactions": transactions_row['Items'],
        "verification": ""
    }
