import boto3
import datetime

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    verification_table = dynamodb.Table('Verifications')
    profile_table = dynamodb.Table('Profile')

    if 'previousState' in event:
        previousState = event['previousState']
    else:
        previousState = 'NONE'

    #  verificationState types: REQUESTED or VERIFIED or FRAUD or NONE
    verification_row = verification_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs, lastUpdate=:lu,"
                         "verifiedBy=:vb, previousState=:ps",
        ExpressionAttributeValues={
            ":vs": event['verificationState'],
            ":lu": datetime.utcnow().isoformat(),
            ":vb": "Admin",
            ":ps": previousState

        },
        ReturnValues="ALL_NEW"
    )

    profile_table.update_item(
        Key={
            "userId": event['userId']
        },
        UpdateExpression="set verificationState=:vs",
        ExpressionAttributeValues={
            "verificationState": event['verificationState']
        },
        ReturnValues="ALL_NEW"
    )

    return verification_row
