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


    data = profile_table.update_item(
        Key={
            "userId": jsonInput["userId"]
        },
        UpdateExpression="set currency=:c, lang=:l, gravatarEmail=:ge",
        ExpressionAttributeValues={
            ":c": jsonInput["currency"],
            ":l": jsonInput["language"],
            ":ge":jsonInput["gravatarEmail"]

        },
        ReturnValues="UPDATED_NEW"
    )

    return {
        'statusCode': 200,
        'body': "success"
    }