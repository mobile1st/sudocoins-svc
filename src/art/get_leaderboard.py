import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    leaderboard = get_config()

    return {
        'artists': leaderboard['collections'],
        "artists_hour": leaderboard['collections_hour'],
        "artists_half": leaderboard['collections_half']
    }

def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'Leaderboard'}
    )['Item']
