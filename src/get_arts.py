import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    if not len(event['arts']) > 0:
        return {
            "arts": "None"
        }

    art_uploads_record = art.get_arts(event['arts'])

    return {
        'arts': art_uploads_record
    }

