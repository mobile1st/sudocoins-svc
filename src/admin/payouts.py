import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    payouts = dynamodb.Table('Payouts')

    pending_payout = payouts.query(
        KeyConditionExpression=Key("status").eq("Pending"),
        ScanIndexForward=False,
        ExpressionAttributeNames={'#s': 'status', '#t': 'type'},
        IndexName='byStatus',
        ProjectionExpression="paymentId, address, amount, lastUpdate, payoutType, "
                             "#s, #t, usdBtcRate, userId, userInput, verificationState")

    return pending_payout["Items"]
