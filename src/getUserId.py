import boto3
import json
import uuid
from datetime import datetime


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    print(event)

    userData = event["request"]["userAttributes"]

    # Get userId from email index
    if 'email' in userData.keys():
        email = userData["email"]

        response = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            })
    # if an email is found, use the userId associated with it.
    if response['Count'] > 0:
        return response['Items']['userId']

    else:
        #  if user signed up with phone number
        return {
            'statusCode': 200,
            'body': userData["sub"]
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
