import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    trades_delta = get_config()

    return {
        'day': trades_delta['day']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'TradesDelta'}
    )['Item']
