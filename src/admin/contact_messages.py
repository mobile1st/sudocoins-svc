import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    contact_table = dynamodb.Table('Contact')
    contact_row = contact_table.query(
        KeyConditionExpression=Key("msgStatus").eq("pending"),
        ScanIndexForward=False,
        IndexName='msgStatus-index',
        ProjectionExpression="msgId, email, transactionId, userId, message, created, Id")

    return contact_row['Items']
