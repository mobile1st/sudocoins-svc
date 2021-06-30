import boto3
import json
import http.client
from util import sudocoins_logger
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
secret = '6LfDfokaAAAAAMYePyids1EPPZ4guZkD6yJHC3Lm'


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    input_json = json.loads(event['body'])
    recaptcha_response = call_google_recaptcha(input_json['token'])
    success_response = recaptcha_response['success']
    user_id = input_json['userId']
    update_profile_with_recaptcha_response(user_id, success_response)

    return update_profile_with_recaptcha_response(user_id, success_response)


def call_google_recaptcha(input_token):
    conn = http.client.HTTPSConnection('www.google.com')
    conn.request('POST', f'/recaptcha/api/siteverify?secret={secret}&response={input_token}')
    response = conn.getresponse()
    json_response = json.loads(response.read())
    log.debug(f'recaptcha response: {json_response}')

    return json_response


def update_verifications_with_recaptcha_response(user_id, response):
    dynamodb.Table('Verifications').update_item(
        Key={
            'userId': user_id
        },
        UpdateExpression='set verificationState=:vs, lastUpdate=:lu,'
                         'verifiedBy=:vb',
        ExpressionAttributeValues={
            ':vs': response,
            ':lu': datetime.utcnow().isoformat(),
            ':vb': 'CashOut'
        },
        ReturnValues='ALL_NEW'
    )


def update_profile_with_recaptcha_response(user_id, response):
    return dynamodb.Table('Profile').update_item(
        Key={
            'userId': user_id
        },
        UpdateExpression="set verificationState=:vs",
        ExpressionAttributeValues={
            ':vs': response
        },
        ReturnValues='ALL_NEW'
    )['Attributes']
