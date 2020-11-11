import boto3
import json


def lambda_handler(event, context):
    """Updates the currency for the user
    Arguments: userId, currency
    Returns: the new currency value
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    subTable = dynamodb.Table("sub")
    jsonInput = event

    sub = event["sub"]

    subResponse = subTable.get_item(Key={'sub': sub})

    userId = subResponse['Item']['userId']

    data = profileTable.update_item(
        Key={
            "userId": userId
        },
        UpdateExpression="set currency=:c",
        ExpressionAttributeValues={
            ":c": jsonInput["currency"]
        },
        ReturnValues="UPDATED_NEW"
    )

    return {
        'body': data["Attributes"]
    }
