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
    json_input = json.loads(event["body"])
    # . json_input = event["body"] #. uncomment for testing

    data = profile_table.update_item(
        Key={
            "userId": json_input["userId"]
        },
        UpdateExpression="set currency=:c, lang=:l, gravatarEmail=:ge",
        ExpressionAttributeValues={
            ":c": json_input["currency"],
            ":l": json_input["language"],
            ":ge": json_input["gravatarEmail"]

        },
        ReturnValues="ALL_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(data["Attributes"])
    }
