import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    trending_art = get_trending()
    arts = []

    for i in trending_art:
        arts.append(i['art_id'])

    set_config(arts)

    return {
        'trending': arts
    }


def set_config(arts):
    config_table = dynamodb.Table('Config')
    updated_art = config_table.update_item(
        Key={
            'configKey': 'TrendingArt'
        },
        UpdateExpression="set art=:art",
        ExpressionAttributeValues={
            ":art": arts
        },
        ReturnValues="ALL_NEW"
    )
    log.info(f'updated_art {updated_art}')


def get_trending():
    # returns art sorted by click_count
    return dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true'),
        ScanIndexForward=False,
        Limit=250,
        IndexName='Trending-index',
        ProjectionExpression="art_id, click_count"
    )['Items']
