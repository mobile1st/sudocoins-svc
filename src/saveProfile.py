import boto3
import json
import uuid
from datetime import datetime


def lambda_handler(event, context):
    """Saves the user profile after registering through Cognito
    Arguments: userId, email, phone, sub, JWT, createdAt, identityProvider, status, ip
    Returns: attributes sa
    """
    dynamodb = boto3.resource('dynamodb')
    profileTable = dynamodb.Table("Profile")
    jsonInput = json.loads(event["body"])
    print(event)
    #. jsonInput = event["body"] #. uncomment for testing

    userId = str(uuid.uuid1())
    ts = str(datetime.utcnow().isoformat())
    userProfile = {
        "userId": userId,
        "email": jsonInput["email"],
        "phone": jsonInput["phone"],
        "sub": jsonInput["sub"]
        "createdAt": ts,
        "identityProvider": jsonInput["identityProvider"],
        "status": True,
        "ip": event['requestContext']['identity']['sourceIp']
    }

    profileResponse = profileTable.put_item(
        Item=userProfile
    )

    return {
        'statusCode': 200,
        'body': "Profile saved"
    }
