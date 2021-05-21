import boto3
from art import Art
import sudocoins_logger
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # returns the art shared by the user

    my_art = art.get_uploads(event['userId'])

    return {
        'statusCode': 200,
        'art': my_art
    }