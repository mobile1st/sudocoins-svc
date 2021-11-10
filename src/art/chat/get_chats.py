import boto3
from util import sudocoins_logger
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')

    art_id = body.get('art_id')
    collection_id = body.get('collection_id')
    timestamp = datetime.utcnow().isoformat()

    if art_id is not None:
        res = dynamodb.Table('chat').query(
            KeyConditionExpression=Key("art_id").eq(art_id) & Key("timestamp").lt(timestamp),
            ScanIndexForward=False,
            Limit=250,
            IndexName='art_id-timestamp-index'
        )

    else:
        res = dynamodb.Table('chat').query(
            KeyConditionExpression=Key("collection_id").eq(collection_id) & Key("timestamp").lt(timestamp),
            ScanIndexForward=False,
            Limit=250,
            IndexName='collection_id-timestamp-index'
        )


    return {
        "chats": res.get('Items')
    }
