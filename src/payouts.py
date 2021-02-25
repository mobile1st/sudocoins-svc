import boto3
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    payouts = dynamodb.Table('Ledger')

    pendingPayouts = payouts.query(
        KeyConditionExpression=Key("status").eq("Pending"),
        ScanIndexForward=False,
        IndexName='byStatus',
        ProjectionExpression="userId, transactionId, amount, lastUpdate, payoutType, "
                             "status, type, usdBtcRate, userInput")

    payoutRecords = pendingPayouts["Items"]

    return payoutRecords
