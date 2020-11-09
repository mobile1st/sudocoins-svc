import boto3
import json


def lambda_handler(event, context):
    """Updates the currency for the user
    Arguments: userId, currency
    Returns: the new currency value
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    jsonInput = event


    data = profileTable.update_item(
        Key={
            "userId": jsonInput["userId"]
        },
        UpdateExpression="set currency=:c",
        ExpressionAttributeValues={
            ":c": jsonInput["currency"]
        },
        ReturnValues="UPDATED_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(data["Attributes"])
    }
