import boto3
import json
import os
from botocore.exceptions import ClientError
import uuid


def lambda_handler(event, context):
    print("event=%s userId=%", event, context.identity.cognito_identity_id)
    """Send contact us message to DB
    Arguments: userId, message
    Returns: success message
    """
    dynamodb = boto3.resource('dynamodb')

    contactTable = dynamodb.Table(os.environ["CONTACT_TABLE"])

    msgId = str(uuid.uuid1())

    jsonInput = event

    message = {
        'msgId': msgId,
        'userId': jsonInput["userId"],
        'message': jsonInput["message"]
    }

    if 'transactionId' in jsonInput:
        message['transactionId'] = jsonInput['transactionId']

    contactResponse = contactTable.put_item(
        Item=message
    )

    return {
        'statusCode': 200,
        'body': "success"
    }
