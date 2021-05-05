import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    trending_art = getConfig()

    art_data = art.get_arts(trending_art[0:10])

    return {
        'top_trending': art_data,
        'remaining': trending_art[10:]
    }


def getConfig():
    configTable = dynamodb.Table('Config')
    configKey = "TrendingArt"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    return config['art']
