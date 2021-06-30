import boto3
import uuid
import json
from datetime import datetime
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    input_json = json.loads(event['body'])
    message = {
        'msgId': str(uuid.uuid1()),
        'userId': input_json['userId'],
        'message': input_json['message'],
        'created': datetime.utcnow().isoformat(),
        'msgStatus': 'pending'
    }

    # probably can be removed
    if 'transactionId' in input_json:
        message['transactionId'] = input_json['transactionId']
    if 'email' in input_json:
        message['email'] = input_json['email']

    dynamodb.Table('Contact').put_item(
        Item=message
    )

    sns_client.publish(
        PhoneNumber="+16282265769",
        Message="Contact us message submitted"
    )
    return {
        'success': True
    }
