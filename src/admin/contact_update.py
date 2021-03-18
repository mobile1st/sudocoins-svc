import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    contact_table = dynamodb.Table('Contact')
    now = str(datetime.utcnow().isoformat())
    contact_table.update_item(
        Key={
            "msgId": event["msgId"]
        },
        UpdateExpression="set msgStatus=:ms, lastUpdate=:lu",
        ExpressionAttributeValues={
            ":ms": "complete",
            ":lu": now

        },
        ReturnValues="ALL_NEW"
    )

    return {
        'body': "Success"
    }
