import boto3
import json
import uuid
from datetime import datetime


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    print(event)

    ts = str(datetime.utcnow().isoformat())
    userData = event["request"]["userAttributes"]
    # Check to see if the registering user has an already registered email
    if 'email' in userData.keys():
        email = userData["email"]

        response = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            })
    # if an email is found, use the existing userId
    if response['Count'] > 0:
        return response['Items']['userId']

    else:
        userProfile = {}
        for i in userData:
            userProfile[i] = userData[i]

        if 'sub' in userProfile:
            userProfile["userId"] = userProfile['sub']
        else:
            userProfile["userId"] = str(uuid.uuid1())

            userProfile['createdAt'] = ts
            userProfile['identityProvider'] = 'Cognito'
            userProfile['status'] = True

            return {
                'statusCode': 200,
                'body': userProfile["userId"]
            }

    '''
    {'userName': 'tedbrink29@gmail.com',
     'request': {
        'userAttributes': {
            'sub': '58ed289b-0bb1-4741-aa9f-3802262d319a',
            'email_verified': 'true',
            'cognito:user_status': 'CONFIRMED',
            'phone_number_verified': 'false',
            'phone_number': '+17329938083',
            'email': 'tedbrink29@gmail.com'}
        }
     }
    '''