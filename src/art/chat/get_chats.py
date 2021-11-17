import boto3
from util import sudocoins_logger
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'event: {event}')
    connectionID = event["requestContext"].get("connectionId")

    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')

    timestamp = datetime.utcnow().isoformat()
    conversationId = body.get("conversationId")

    res = dynamodb.Table('chat').query(
            KeyConditionExpression=Key("conversationId").eq(conversationId) & Key("timestamp").lt(timestamp),
            ScanIndexForward=False,
            Limit=20,
            IndexName='conversationId-timestamp-index'
    )

    for i in res.get('Items'):
        _send_to_connection(connectionID, i, event)
        log.info('message sent')

    return {
        "status": 200
    }


def _send_to_connection(connection_id, data, event):
    endpoint = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]

    gatewayapi = boto3.client("apigatewaymanagementapi",
                              endpoint_url=endpoint)
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
                                         Data=json.dumps(data).encode('utf-8'))
