import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    input_json = json.loads(event.get('body', '{}'))
    # . input_json = event

    sub = input_json.get('sub')
    collection_code = input_json.get('collection_id')

    if input_json['action'] == "add":
        try:
            dynamodb.Table('portfolio').put_item(
                Item={
                    'user_id': sub, 'collection_code': collection_code
                },
                ConditionExpression='attribute_not_exists(user_id) AND attribute_not_exists(collection_code)'
            )
            log.info("added")

            update_expression = "ADD portfolio :i"
            attribute_values = {":i": set([collection_code])}  # {':res': [collection_code],':el': []}
            var = dynamodb.Table('sub').update_item(
                Key={'sub': sub},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
            )['Attributes']['portfolio']

            log.info("portfolio added to sub table")

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
            update_expression = "ADD portfolio :i"
            # 'SET portfolio = list_append(if_not_exists(portfolio, :el), :res)'
            attribute_values = {":i": set([collection_code])}  # {':res': [collection_code],':el': []}

            var = dynamodb.Table('sub').update_item(
                Key={'sub': sub},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
            )['Attributes']['portfolio']

            collections = []
            for i in var:
                collections.append(i)

            return {
                "portfolio": collections

            }


    elif input_json['action'] == "delete":
        response = dynamodb.Table('portfolio').delete_item(
            Key={
                'user_id': sub,
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
