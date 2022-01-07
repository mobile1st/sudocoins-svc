import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    trades_delta = get_config()

    return {
        'day': trades_delta['day'],
        'hour': trades_delta['hour'],
        'week':trades_delta['week']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'TradesDelta'}
    )['Item']
