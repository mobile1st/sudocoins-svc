import boto3
import json
import uuid
from datetime import datetime


def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    print(event)

    userData = event["request"]["userAttributes"]

    # find the userId mapped to the email
    if 'email' in userData.keys():
        email = userData["email"]

        response = profileTable.query(
            IndexName='email-index',
            KeyConditionExpression='email = :email',
            ExpressionAttributeValues={
                ':email': email
            })

    # if an email is found, use the userId
    if response['Count'] > 0:

        return {
            'statusCode': 200,
            'body': response['Items'][0]['userId']
        }

    # if an email isn't found, use the sub id (shouldn't be, unless user registered with phone only)
    else:

        return {
            'statusCode': 200,
            'body': userData["sub"]
        }
