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

    message = {
        'msgId': msgId,
        'userId': event["userId"],
        'message': event["message"],
        'created': timeNow,
        'msgStatus': "pending"
    }

    if 'transactionId' in event:
        message['transactionId'] = event['transactionId']
    if 'email' in event:
        message['email'] = event['email']

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
