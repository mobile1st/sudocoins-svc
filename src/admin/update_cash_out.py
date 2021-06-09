import boto3
import json
from art import history
from datetime import datetime
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
profile = history.History(dynamodb)


def lambda_handler(event, context):
    body = json.loads(event['body'])
    log.debug(f'data: {body}')
    success = []
    fail = []

    for i in body['records']:
        transaction_id = i['transactionId']
        try:
            user_id = i['userId']
            user_status = 'Complete' if 'userStatus' not in i else i['userStatus']
            update_cash_out(user_id, transaction_id, user_status)
            success.append(transaction_id)
        except Exception:
            log.exception('could not update cash out')
            fail.append(transaction_id)

    return {
        'success': success,
        'fail': fail
    }


def update_cash_out(user_id, transaction_id, user_status):
    ledger_table = dynamodb.Table('Ledger')
    payouts_table = dynamodb.Table('Payouts')

    now = str(datetime.utcnow().isoformat())

    payouts_table.update_item(
        Key={
            "paymentId": transaction_id
        },
        UpdateExpression="set #s=:s, lastUpdate=:lu",
        ExpressionAttributeValues={
            ":s": user_status,
            ":lu": now
        },
        ExpressionAttributeNames={'#s': 'status'}
    )

    ledger_table.update_item(
        Key={
            "userId": user_id,
            "transactionId": transaction_id
        },
        UpdateExpression="set #s=:s, lastUpdate=:lu",
        ExpressionAttributeValues={
            ":s": user_status,
            ":lu": now

        },
        ExpressionAttributeNames={'#s': 'status'}
    )

    profile.updateProfile(user_id)
