import boto3
from util import sudocoins_logger
import json
from datetime import datetime
import uuid
import http.client

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))
    log.info(f'payload: {body}')

    if body.get("detail", {}).get("type", "") == "message":
        try:
            captchaToken = body.get("captchaToken")
            recaptcha_response = call_google_recaptcha(captchaToken)
            success_response = recaptcha_response['success']
            if success_response is False:
                return

        except Exception as e:
            log.info(e)

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
        try:
            log.info(connectionID)
            _send_to_connection(connectionID, chat, event)
            log.info('sent to client')
        except Exception as e:
            log.info(e)

    return {
        "statusCode": 200,
        "body": "Message sent to connections"
    }


def _send_to_connection(connection_id, data, event):
    endpoint = "https://" + event["requestContext"]["domainName"] + "/" + event["requestContext"]["stage"]

    gatewayapi = boto3.client("apigatewaymanagementapi",
                              endpoint_url=endpoint)
    return gatewayapi.post_to_connection(ConnectionId=connection_id,
                                         Data=json.dumps(data).encode('utf-8'))


def call_google_recaptcha(input_token):
    secret = "6Ledii4dAAAAAKvn9t-Y1RvfeR7nmNAnZGejK1P_"
    conn = http.client.HTTPSConnection('www.google.com')
    conn.request('POST', f'/recaptcha/api/siteverify?secret={secret}&response={input_token}')
    response = conn.getresponse()
    json_response = json.loads(response.read())
    log.debug(f'recaptcha response: {json_response}')

    return json_response

