import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    macro = get_config()

    return {
        'day': macro['day'],
        'hour': macro['hour'],
        'week': macro['week'],
        'index_data': macro['index_data'],
        'daily_data': macro['daily_data']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'macro_stats'}
    )['Item']
