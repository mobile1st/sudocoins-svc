import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    volume_delta = get_config()

    return {
        'day': volume_delta['day'],
        'hour': volume_delta['hour'],
        'week': volume_delta['week']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'VolumeDelta'}
    )['Item']
