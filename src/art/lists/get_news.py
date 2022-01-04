import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):

    timestamp = str(datetime.today().strftime('%Y-%m-%d'))

    news = dynamodb.Table('news').query(
        KeyConditionExpression=Key("approved").eq('true') & Key("pubDate").lt(timestamp),
        ScanIndexForward=False,
        IndexName='approved-pubDate-index'
    )['Items']

    return {
        "news": news
    }
