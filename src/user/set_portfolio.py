import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    # input_json = json.loads(event.get('body', '{}'))
    input_json = event

    sub = input_json.get('sub')
    collection_code = input_json.get('collection_id')

    if input_json['action'] == "add":
        try:

            update_expression = 'SET user_id =:pk, collection_id=:cid'
            attribute_values = {
                ':pk': sub,
                ':cid': collection_code
            }
            response = dynamodb.Table('portfolio').put_item(
                Item={
                    'user_id': sub, 'collection_code': collection_code
                },
                ConditionExpression='attribute_not_exists(user_id) AND attribute_not_exists(collection_code)'

            )
            log.info("added")
            # log.info(f'add response: {response}')

            update_expression = "ADD portfolio :i"
            # 'SET portfolio = list_append(if_not_exists(portfolio, :el), :res)'
            attribute_values = {":i": set([collection_code])}  # {':res': [collection_code],':el': []}

            var = dynamodb.Table('sub').update_item(
                Key={'sub': sub},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
            )['Attributes']['portfolio']

            log.info("added to sub")
            tmp = type(var)
            log.info(f'profile update response: {tmp}')

            collections = []
            for i in var:
                log.info(i)
                tmp = type(i)
                log.info(f'profile update response: {tmp}')
                collections.append(i)

            return {
                "portfolio": collections

            }

        except Exception as e:
            log.info(e)
            return {}

    elif input_json['action'] == "delete":
        response = dynamodb.Table('portfolio').delete_item(
            Key={
                'user_id': user_id,
                'collection_code': collection_code
            },
            ProjectionExpression='collection_code',
            ReturnValues='UPDATED_ALL'
        )['Attributes']

        log.info(f'delete response: {response}')

        update_expression = 'SET portfolio=:res'
        attribute_values = {
            ':res': response
        }
        var = dynamodb.Table('profile').update_item(
            Key={'user_id': user_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values
        )['Attributes']

        log.info(f'profile update response: {var}')

        return var

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
