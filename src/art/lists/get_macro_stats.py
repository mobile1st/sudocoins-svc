import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    macro = get_config()

    return {
        'points': macro['points'],
        'index_points': macro['index_points']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'macro_stats'}
    )['Item']
