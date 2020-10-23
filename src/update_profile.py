import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
import uuid


def lambda_handler(event, context):
    print(event)
    dynamodb = boto3.resource('dynamodb')
    profile_table = dynamodb.Table("Profile")

    json_input = json.loads(event["body"])

    data = profile_table.update_item(
        Key={
            "UserId": json_input["UserId"]
        },
        UpdateExpression="set currency=:c, lang=:l, gravatarEmail=:ge",
        ExpressionAttributeValues={
            ":c": json_input["currency"],
            ":l": json_input["language"],
            ":ge": json_input["gravatarEmail"]

        },
        ReturnValues="UPDATED_NEW"
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Profile updated')
    }
