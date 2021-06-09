from datetime import datetime
import boto3
import uuid
import json
from history import History
from util import sudocoins_logger

log = sudocoins_logger.get()
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
history = History(dynamodb)


def lambda_handler(event, context):
    try:
        log.debug(f'event: {event}')
        cash_out_data = json.loads(event['body'])
        requested_sub = cash_out_data['sub']
        jwt_sub_claim = event['requestContext']['authorizer']['jwt']['claims']['sub']
        if requested_sub != jwt_sub_claim:
            return error_response('Authorization check failed for the request')

        user_id, verification_state, sudocoins = load_profile(requested_sub)
        if cash_out_data['amount'] > sudocoins:
            return True, error_response("The requested amount is higher than the user's balance")

        profile = cash_out(cash_out_data, user_id, verification_state)

        return {
            'error': False,
            'sudocoins': 0,
            'history': profile.get('history', []),
            'balance': 0.0
        }
    except Exception:
        msg = 'Error during cash out'
        log.exception(msg)
        send_notification(msg)
        return error_response(msg)


def error_response(msg):
    return {
        'error': True,
        'message': msg,
    }


def cash_out(cash_out_data, user_id, verification_state):
    amount = cash_out_data['amount']
    cash_out_type = cash_out_data['type']
    rate = cash_out_data['rate']
    sudo_rate = cash_out_data['sudoRate']
    address = cash_out_data['address']

    last_update = datetime.utcnow().isoformat()
    transaction_id = str(uuid.uuid1())

    dynamodb.Table('Payouts').put_item(
        Item={
            'paymentId': transaction_id,
            'userId': user_id,
            'amount': amount,
            'lastUpdate': last_update,
            'type': cash_out_type,
            'status': 'Pending',
            'usdBtcRate': rate,
            'sudoRate': sudo_rate,
            'verificationState': verification_state,
            'address': address
        }
    )
    dynamodb.Table('Ledger').put_item(
        Item={
            'userId': user_id,
            'amount': amount,
            'lastUpdate': last_update,
            'type': 'Cash Out',
            'status': 'Pending',
            'transactionId': transaction_id,
            'usdBtcRate': rate,
            'verificationState': verification_state,
            'payout_type': cash_out_type
        }
    )
    profile_table = dynamodb.Table('Profile')
    profile_table.update_item(
        Key={'userId': user_id},
        UpdateExpression="SET sudocoins = :val",
        ExpressionAttributeValues={
            ':val': 0
        }
    )

    send_notification('Cash Out submitted')

    history.updateProfile(user_id)

    return profile_table.get_item(
        Key={'userId': user_id},
        ProjectionExpression="history"
    )['Item']


def load_profile(sub):
    sub_table = dynamodb.Table('sub')
    profile_table = dynamodb.Table('Profile')

    sub_item = sub_table.get_item(Key={'sub': sub})['Item']
    user_id = sub_item['userId']

    profile_item = profile_table.get_item(Key={'userId': user_id})['Item']
    verification_state = profile_item.get('verificationState')
    sudocoins = profile_item.get('sudocoins')

    return user_id, verification_state, sudocoins


def send_notification(msg):
    log.debug(f'sending message: {msg}')
    sns_client.publish(
        PhoneNumber="+16282265769",
        Message=msg
    )
