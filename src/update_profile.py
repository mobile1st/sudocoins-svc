import json
from datetime import datetime
import boto3
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps('Profile updated')
    }
