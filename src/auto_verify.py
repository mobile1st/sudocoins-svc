import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    verification_table = dynamodb.Table('Verifications')
    profile_table = dynamodb.Table('Profile')

    print(event)

    # receive and process request from Google recaptcha
    # call recaptcha API and determine if user is human or bot/fraud
    # set state of result to verificationState


    verificationState = "VERIFIED"

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

    userProfile = profile_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs",
        ExpressionAttributeValues={
            ":vs": verificationState
        },
        ReturnValues="ALL_NEW"
    )


    return userProfile['Attributes']

