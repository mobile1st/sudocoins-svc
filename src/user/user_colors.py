import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    print("hi")

    log.info(f'event: {event}')
    # input_json = json.loads(event.get('body', '{}'))
    input_json = event

    if input_json['msg'] == "update":
        return update_colors(input_json['user_id'], input_json['bg_color'],
                             input_json['tile_color'], input_json['text_color'])

    elif input_json['msg'] == "get":
        return get_colors(input_json['user_id'])

    return


def update_colors(user_id, bg_color, tile_color, text_color):
    update_expression = 'SET bg_color=:bg, tile_color=:tc, text_color=:txt'
    attribute_values = {
        ':bg': bg_color,
        ':tc': tile_color,
        ':txt': text_color
    }
    return dynamodb.Table('Profile').update_item(
        Key={
            'userId': user_id
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=attribute_values,
        ReturnValues='UPDATED_NEW'
    )['Attributes']


def get_colors(user_id):
    return dynamodb.Table('Profile').get_item(Key={'userId': user_id},
                                              ProjectionExpression='userId, bg_color, text_color, tile_color')['Item']
