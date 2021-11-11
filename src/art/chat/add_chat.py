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

    table = dynamodb.Table("chat_connections")
    response = table.scan(ProjectionExpression="ConnectionID")
    items = response.get("Items", [])
    connections = [x["ConnectionID"] for x in items if "ConnectionID" in x]

    for connectionID in connections:
        _send_to_connection(connectionID, chat, event)

    return


def _send_to_connection(connection_id, data, event):
    gatewayapi = boto3.client("apigatewaymanagementapi",
            endpoint_url = "https://" + event["requestContext"]["domainName"] +
                    "/" + event["requestContext"]["stage"])
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
            Data=json.dumps(data).encode('utf-8'))

