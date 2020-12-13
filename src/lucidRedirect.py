import json
import os
import boto3


def lambda_handler(event, context):
    print(event)

    response = {
        "statusCode": 302,
        "headers": {'Location': 'https://www.sudocoins.com/'},
        "body": json.dumps({})
    }

    return response
