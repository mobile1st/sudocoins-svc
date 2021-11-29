import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):

    pending_upcoming = dynamodb.Table('upcoming').query(
        KeyConditionExpression=Key("approved").eq('false'),
        ScanIndexForward=False,
        IndexName='pending-approved-index'
    )['Items']

    return pending_upcoming

