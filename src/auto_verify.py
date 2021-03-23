import boto3
from datetime import datetime
import requests
import json

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    verification_table = dynamodb.Table('Verifications')
    profile_table = dynamodb.Table('Profile')

    print(event)

    url = 'https://www.google.com/recaptcha/api/siteverify'
    myobj = {
        "secret": "6LfDfokaAAAAAMYePyids1EPPZ4guZkD6yJHC3Lm",
        "response": event['token']
    }
    x = requests.post(url, data=myobj)
    response = json.loads(x.text)

    print(response['success'])

    verificationState = "FRAUD"

    if response['success'] == "False":
        verificationState = "FRAUD"

    elif response['success'] == "True":
        verificationState = "VERIFIED"

    userId = event['userId']

    if 'userId' in event:
        userId = event['userId']
    else:
        userId = "64e1975c-2a94-11eb-9ec8-2d6e99b50af0"

    print(verificationState)

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

