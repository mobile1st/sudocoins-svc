import boto3
import datetime

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    verification_table = dynamodb.Table('Verifications')
    profile_table = dynamodb.Table('Profile')

    # receive and process request from Google recaptcha
    # call recaptcha API and determine if user is human or bot/fraud
    # set state of result to verificationState
    verificationState = ""

    verification_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs, lastUpdate=:lu,"
                         "verifiedBy=:vb",
        ExpressionAttributeValues={
            ":vs": verificationState,
            ":lu": datetime.utcnow().isoformat(),
            ":vb": "CashOut"

        },
        ReturnValues="ALL_NEW"
    )

    profile_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs",
        ExpressionAttributeValues={
            "verificationState": verificationState
        },
        ReturnValues="ALL_NEW"
    )

    return verificationState

