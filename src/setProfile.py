import boto3
import json
import uuid
from datetime import datetime


def lambda_handler(event, context):

    print(event)

    return {
        'statusCode': 200,
        'body': "Profile saved"
    }
