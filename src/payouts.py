import boto3
from boto3.dynamodb.conditions import Key, Attr


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    payouts = dynamodb.Table('Payouts')

    pendingPayouts = payouts.query(
        KeyConditionExpression=Key("status").eq("Pending"),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='byStatus',
        ProjectionExpression="paymentId, address, amount, lastUpdate, payoutType, "
                             "#s, #t, usdBtcRate, userId, userInput")

    payoutRecords = pendingPayouts["Items"]

    return payoutRecords
