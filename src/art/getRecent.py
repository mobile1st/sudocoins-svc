import boto3
import sudocoins_logger
from art.art import Art
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    # need to make sure list doesn't contain duplicates or the batch function will break

    time_now = str(datetime.utcnow().isoformat())
    query_params = event['queryStringParameters']
    count = int(query_params['count'])
    from_utc = datetime.fromtimestamp(int(query_params['timestamp']) / 1000).isoformat()
    recent_art = art.get_recent(count, from_utc)
    # event["time_now"]

    return {
        'art': recent_art['Items']
    }
