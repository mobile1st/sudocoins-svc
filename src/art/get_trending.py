import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    trending_art = get_config()

    return {
        'trending': trending_art
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'TrendingArt'}
    )['Item']
