import boto3
from art import Art
import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # need to make sure list doesn't contain duplicates or the batch function will break

    recent_art = art.get_recent(event['count'])

    return {
        'statusCode': 200,
        'art': recent_art
    }