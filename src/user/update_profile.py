import boto3
import json
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    input_json = json.loads(event.get('body', '{}'))

    user_id = input_json['userId'] if 'userId' in input_json else get_user_id(input_json['sub'])

    current_user_name = get_current_user_name(user_id)
    user_name = input_json.get('user_name')

    if current_user_name == user_name:
        profile = update_profile(
            user_id,
            input_json.get('gravatarEmail'),
            user_name,
            input_json.get('twitter_handle')
        )
        return {
            'profile': profile
        }


    if check_user_name_exists(user_name):
        return {
            'message': 'User Name already exists. Please try something different.'
        }

    profile = update_profile(
        user_id,
        input_json.get('gravatarEmail'),
        user_name,
        input_json.get('twitter_handle')
    )
    return {
        'profile': profile
    }


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def check_user_name_exists(user_name):
    if not user_name:
        return False

    return dynamodb.Table('Profile').query(
        IndexName='user_name-index',
        KeyConditionExpression='user_name = :user',
        ExpressionAttributeValues={
            ':user': user_name
        }
    )['Count'] > 0


def update_profile(user_id, gravatar_email, user_name, twitter_handle):
    update_expression = 'SET gravatarEmail=:ge, twitter_handle=:th'
    attribute_values = {
        ':ge': gravatar_email,
        ':th': twitter_handle
    }
    if user_name:  # user_name is also an index, can't update with null
        update_expression = 'SET gravatarEmail=:ge, user_name=:un, twitter_handle=:th'
        attribute_values[':un'] = user_name
    return dynamodb.Table('Profile').update_item(
        Key={
            'userId': user_id
        },
        UpdateExpression=update_expression,
        ExpressionAttributeValues=attribute_values,
        ReturnValues='ALL_NEW'
    )['Attributes']


def get_user_id(sub):
    return dynamodb.Table('sub').get_item(Key={'sub': sub})['Item']['userId']


def get_current_user_name(user_id):
    row = dynamodb.Table('profile').get_item(Key={'userId': user_id})['Item']
    if 'user_name' in row:
        return row['user_name']
    else:
        user_name = None
        return user_name

