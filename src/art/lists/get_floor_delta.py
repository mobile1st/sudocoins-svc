import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    floor_delta = get_config()

    return {
        'day': floor_delta['day'],
        'hour': floor_delta['hour'],
        'week': floor_delta['week']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'FloorDelta'}
    )['Item']
