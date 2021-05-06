import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):

    trending_art = getConfig()

    return {
        'trending': trending_art
    }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "TrendingArt"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config['art']