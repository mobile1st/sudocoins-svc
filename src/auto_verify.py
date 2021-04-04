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

    userId = event['userId']

    verification_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs, lastUpdate=:lu,"
                         "verifiedBy=:vb",
        ExpressionAttributeValues={
            ":vs": response['success'],
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
            ":vs": response['success']
        },
        ReturnValues="ALL_NEW"
    )

    return userProfile['Attributes']

