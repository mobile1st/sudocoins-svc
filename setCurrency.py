import boto3
from botocore.exceptions import ClientError
import uuid
import json


def lambda_handler(event, context):
    """Updates the currency for the user
    Arguments: userId
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
        UpdateExpression="set currency=:c",
        ExpressionAttributeValues={
            ":c": json_input["currency"]
        },
        ReturnValues="ALL_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps(data["Attributes"])
    }
