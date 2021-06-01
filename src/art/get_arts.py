import boto3
import json
from art.art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    body = json.loads(event['body'])
    arts = body['arts']
    if len(arts) == 0:
        return {'arts': None}

    art_uploads_record = art.get_arts(arts)

    return {
        'arts': art_uploads_record
    }
