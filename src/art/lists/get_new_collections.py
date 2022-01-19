import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    new_collections = get_config()

    return {
        'day': new_collections['day1'],
        "day3": new_collections['day3'],
        "week": new_collections['day7']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'NewCollections'}
    )['Item']
