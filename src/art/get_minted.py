import boto3
from util import sudocoins_logger
from art.art import Art
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    #  query_params = event['queryStringParameters']
    count = 100
    timestamp = str(datetime.utcnow().isoformat())
    return {
        'art': arts.get_minted(count, timestamp)
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
