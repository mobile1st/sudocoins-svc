import boto3

dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    chart_data = get_config()

    return {
        'chart_data': chart_data['data_points']
    }


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'time_series'}
    )['Item']
