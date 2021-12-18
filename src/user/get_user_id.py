import boto3
import uuid
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    public_address = event['pathParameters']['publicAddress']

    global user_id
    if public_address != '':
        try:
            user_id = get_user_id(public_address)
        except Exception as e:
            log.exception(e)
            user_id = ''
    return {
        'status': 200,
        'userId': user_id
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_user_id(public_address):
    sub_table = dynamodb.Table('sub')
    sub_response = sub_table.get_item(Key={'sub': public_address})

    if 'Item' in sub_response:
        log.debug("found userid matching publicAddress")
        user_id = sub_response['Item'].get('userId')
        if user_id:
            return user_id
        else:
            log.debug("no sub or email found in database. New user.")

            user_id = str(uuid.uuid1())
            log.debug("completely new user with no email in cognito")

            update_expression = "SET userId=uid"
            attribute_values = {":uid": user_id}
            dynamodb.Table('sub').update_item(
                Key={'sub': public_address},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=attribute_values
            )

            return user_id
    else:
        user_id = str(uuid.uuid1())
        log.debug("completely new user with no email in cognito")
        sub_table.put_item(
            Item={
                "sub": public_address,
                "userId": user_id
            }
        )
        return user_id