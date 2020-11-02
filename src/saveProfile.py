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
    # . json_input = event["body"] #. uncomment for testing

    userId = uuid.uuid1()
    ts = str(datetime.utcnow().isoformat())
    data = {
        "userId": userId,
        "email": jsonInput["email"],
        "phone": jsonInput["phone"],
        "sub": jsonInput["sub"],
        "jwt": jsonInput["jwt"],
        "createdAt": ts,
        "identityProvider": jsonInput["identityProvider"],
        "status": True,
        "ip": jsonInput["ip"]
    }

    data = profileTable.put_item(

    )

    return {
        'statusCode': 200,
        'body': json.dumps(data["Attributes"])
    }
