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
        return response['Items'][0]['userId']
    # if en email isn't found, create a new record in the profile table
    else:
        userProfile = {}
        for i in userData:
            userProfile[i] = userData[i]

        userProfile['createdAt'] = ts
        userProfile['identityProvider'] = 'Cognito'
        userProfile['status'] = True

        if 'sub' in userProfile:
            userProfile["userId"] = userProfile['sub']

        else:
            userProfile["userId"] = str(uuid.uuid1())

        print(userProfile)

        newRecord = profileTable.put_item(
            Item=userProfile)

        return {
            'statusCode': 200,
            'body': userProfile["userId"]
        }
