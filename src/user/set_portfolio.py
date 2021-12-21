import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    # . input_json = json.loads(event.get('body', '{}'))
    input_json = event

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
            log.info("collection added to portfolio")

            update_expression = "ADD portfolio :i"
            attribute_values = {":i": set([collection_code])}  # {':res': [collection_code],':el': []}
            var = dynamodb.Table('sub').update_item(
                Key={'sub': sub},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
            )['Attributes']['portfolio']

            log.info("portfolio updated in sub table")

            collections = []
            for i in var:
                collections.append(i)

            return {
                "portfolio": collections

            }

        except Exception as e:
            log.info(e)
            update_expression = "ADD portfolio :i"
            attribute_values = {":i": set([collection_code])}
            var = dynamodb.Table('sub').update_item(
                Key={'sub': sub},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values,
                ReturnValues="UPDATED_NEW"
            )['Attributes']['portfolio']

            log.info("portfolio updated in sub table")

            collections = []
            for i in var:
                collections.append(i)

            return {
                "portfolio": collections

            }


    elif input_json['action'] == "delete":
        dynamodb.Table('portfolio').delete_item(
            Key={
                'user_id': sub,
                'collection_code': collection_code
            }
        )

        log.info("collection removed from portfolio table")

        update_expression = "DELETE portfolio :i"
        attribute_values = {":i": set([collection_code])}
        var = dynamodb.Table('sub').update_item(
            Key={'sub': sub},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=attribute_values,
            ReturnValues="UPDATED_NEW"
        )

        log.info("portfolio updated in sub table")

        collections = []
        if 'Attributes' in var and 'portfolio' in var['Attributes']:
            for i in var['Attributes']['portfolio']:
                collections.append(i)

        return {
            "portfolio": collections
        }

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


