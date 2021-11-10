import boto3
from util import sudocoins_logger
import json
from datetime import datetime
import uuid

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')

    chat = {
        "chat_id": str(uuid.uuid1()),
        "timestamp": datetime.utcnow().isoformat(),
        "message": body.get("message", ""),
        "art_id": body.get("art_id", "0"),
        "collection_id": body.get("collection_id", "0"),
        "user_id": body.get("user_id", "unknown")
    }

    dynamodb.Table('chat').put_item(
        Item=chat
    )

    return

