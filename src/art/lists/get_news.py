import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    timestamp = str(datetime.utcnow().isoformat())
    log.info("timestamp")

    news = dynamodb.Table('news').query(
        KeyConditionExpression=Key("approved").eq('true') & Key("pubDate").lt(timestamp),
        ScanIndexForward=False,
        IndexName='approved-pubDate-index'
    )['Items']

    news = list({v['link']: v for v in news}.values())

    return {
        "news": news
    }
