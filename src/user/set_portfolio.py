import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    input_json = json.loads(event.get('body', '{}'))

    user_id = input_json.get('sub')
    collection_code = input_json.get('collection_id')

    if input_json['action'] == "add":
        update_expression = 'SET user_id =:pk, collection_id=:cid'
        attribute_values = {
            ':pk': user_id,
            ':cid': collection_code
        }
        response = dynamodb.Table('portfolio').update_item(
            Key={
                'user_id': user_id, 'collection_code': collection_code
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values,
            ProjectionExpression='collection_code',
            ReturnValues='UPDATED_ALL'
        )['Attributes']

        update_expression = 'SET portfolio=:res'
        attribute_values = {
            ':res': response
        }

        var = dynamodb.Table('profile').update_item(
            Key={'user_id': user_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values
        )['Attributes']

        return response

    elif input_json['action'] == "delete":
        response = dynamodb.Table('portfolio').delete_item(
            Key={
                'user_id': user_id,
                'collection_code': collection_code
            },
            ProjectionExpression='collection_code',
            ReturnValues='UPDATED_ALL'
        )['Attributes']

        update_expression = 'SET portfolio=:res'
        attribute_values = {
            ':res': response
        }
        var = dynamodb.Table('profile').update_item(
            Key={'user_id': user_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values
        )['Attributes']

        return response

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


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
