import boto3
import sudocoins_logger
import uuid
from datetime import datetime

log = sudocoins_logger.get()

def lambda_handler(event, context):
    log.debug(f'event: {event}')
    dynamodb = boto3.resource('dynamodb')
    contactTable = dynamodb.Table('Contact')

    msgId = str(uuid.uuid1())
    timeNow = datetime.utcnow().isoformat()

    message = {
        'msgId': msgId,
        'message': event,
        'created': timeNow,
        'msgStatus': "gdpr callback"
    }

    contactTable.put_item(
        Item=message
    )

    return {
        'statusCode': 200,
        'body': "success"
    }
