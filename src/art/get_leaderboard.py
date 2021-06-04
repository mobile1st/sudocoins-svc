import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    leaderboard = getConfig()

    return {
        'leaderboard': leaderboard
    }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "Leaderboard"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config['leaders']

