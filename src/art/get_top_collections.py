import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    top_collections = get_config()

    return {
        'day': top_collections['day'],
        'week': top_collections['week'],
        'hour': top_collections['hour']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'TopCollections'}
    )['Item']
