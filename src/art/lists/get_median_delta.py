import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    median_delta = get_config()

    return {
        'day': median_delta['day'],
        'hour': median_delta['hour'],
        'week': median_delta['week']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'MedianDelta'}
    )['Item']
