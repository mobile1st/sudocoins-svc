import boto3
from botocore.exceptions import ClientError
import uuid
import json


def lambda_handler(event, context):
    """Updates the profile for a registered users.
    Arguments: currency, language, gravatarEmail
    Returns: fields updated
    """
    dynamodb = boto3.resource('dynamodb')
    profile_table = dynamodb.Table("Profile")
    jsonInput = event
    sub = jsonInput["sub"]
    userId = loadProfile(sub)

    if userId is not None:
        data = profile_table.update_item(
            Key={
                "userId": userId
            },
            UpdateExpression="set currency=:c, gravatarEmail=:ge",
            ExpressionAttributeValues={
                ":c": jsonInput["currency"],
                ":ge": jsonInput["gravatarEmail"]

            },
            ReturnValues="UPDATED_NEW"
        )

        return {
            'statusCode': 200,
            'body': "success"
        }

    else:

        return {
            'statusCode': 200,
            'body': "userId not found"
        }



def loadProfile(sub):
    """Fetches user preferences for the Profile page.
    Argument: userId. This may change to email or cognito sub id .
    Returns: a dict mapping user attributes to their values.
    """
    dynamodb = boto3.resource('dynamodb')
    subTable = dynamodb.Table('sub')
    profileTable = dynamodb.Table('Profile')

    subResponse = subTable.get_item(Key={'sub': sub})

    if 'Item' in subResponse:
        userId = subResponse['Item']['userId']

        return userId

    else:

        return None
