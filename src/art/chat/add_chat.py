import boto3
from util import sudocoins_logger
import json
from datetime import datetime
import uuid

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')

    if body.get("detail", {}).get("type", "") == "message":
        chat = {
            "chat_id": str(uuid.uuid1()),
            "timestamp": datetime.utcnow().isoformat(),
            "message": body.get("detail", {}).get("message", ""),
            "conversationId": body.get("detail", {}).get("conversationId", ""),
            "userId": body.get("detail", {}).get("userId", "")
        }
        dynamodb.Table('chat').put_item(
            Item=chat
        )

        del chat['chat_id']
        del chat['timestamp']
        chat['type'] = 'message'

    elif body.get("detail", {}).get("type", "") == "typing":
        chat = {
            "type": "typing",
            "conversationId": body.get("detail", {}).get("conversationId", ""),
            "userId": body.get("detail", {}).get("userId", ""),
            "isTyping": body.get("detail", {}).get("isTyping", ""),
            "content": body.get("detail", {}).get("isTyping", "")
        }


    table = dynamodb.Table("chat_connections")
    response = table.scan(ProjectionExpression="ConnectionId")
    items = response.get("Items", [])
    connections = [x["ConnectionId"] for x in items if "ConnectionId" in x]

    chat = {
        "detail": chat
    }

    for connectionID in connections:
        _send_to_connection(connectionID, chat, event)
        log.info('sent to client')

    return {
        "statusCode": 200,
        "body": "Message sent to connections"
    }


def _send_to_connection(connection_id, data, event):
    endpoint="https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]
    log.info(f'url: {endpoint}')
    gatewayapi = boto3.client("apigatewaymanagementapi",
            endpoint_url=endpoint)
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
            Data=json.dumps(data).encode('utf-8'))

