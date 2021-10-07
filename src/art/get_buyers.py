import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    top_buyers = get_config()

    return {
        'buyers_day': top_buyers['buyers_day'],
        "buyers_half": top_buyers['buyers_half'],
        "buyers_hour": top_buyers['buyers_hour']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'TopBuyers'}
    )['Item']
