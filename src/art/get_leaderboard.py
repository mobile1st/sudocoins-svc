import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
    leaderboard = getConfig()

    return {
        'influencers': leaderboard['leaders'],
        'artists': leaderboard['creators']
    }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "Leaderboard"

    response = configTable.get_item(Key={'configKey': configKey})
    leaderboard = response['Item']

    return leaderboard

