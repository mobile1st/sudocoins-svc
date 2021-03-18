import boto3
import os
import uuid
from datetime import datetime


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)

    dynamodb = boto3.resource('dynamodb')
    contactTable = dynamodb.Table(os.environ["CONTACT_TABLE"])
    profileTable = dynamodb.Table("Profile")
    msgId = str(uuid.uuid1())
    timeNow = datetime.utcnow().isoformat()
    jsonInput = event

    profileObject = profileTable.get_item(
        Key={'userId': jsonInput['userId']},
        ProjectionExpression="active, email, signupDate, userId,"
                             "consent, balance,"
                             "verificationState, signupMethod"
    )

    message = {
        'msgId': msgId,
        'userId': jsonInput["userId"],
        'message': jsonInput["message"],
        'created': timeNow,
        'status': "Unread"
    }

    if 'transactionId' in jsonInput:
        message['transactionId'] = jsonInput['transactionId']
    if 'email' in jsonInput:
        message['email'] = jsonInput['email']

    contactResponse = contactTable.put_item(
        Item=message
    )

    client = boto3.client("sns")
    client.publish(
        PhoneNumber="+16282265769",
        Message="Contact us message submitted"
    )

    return {
        'statusCode': 200,
        'body': "success"
    }
