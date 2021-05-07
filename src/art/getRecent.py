import boto3
from art import Art
import sudocoins_logger
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # need to make sure list doesn't contain duplicates or the batch function will break

    time_now = str(datetime.utcnow().isoformat())

    recent_art = art.get_recent(event['count'], event['timestamp'])
    # event["time_now"]

    return {
        'statusCode': 200,
        'art': recent_art['Items']
    }