import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)

def lambda_handler(event, context):
    set_log_context(event)
    query_params = event['queryStringParameters']
    count = int(query_params['count'])
    return {
        'art': arts.get_recent(count, query_params['timestamp'])
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
