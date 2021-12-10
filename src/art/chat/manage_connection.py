import boto3
from util import sudocoins_logger
import json
from datetime import datetime
import uuid

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event.get('body', '{}'))
    log.info(f'event: {event}')
    log.info(f'payload: {body}')

    """
    Handles connecting and disconnecting for the Websocket.
    Adds the connectionID to the database.
    Disconnect removes the connectionID from the database.
    """
    connectionID = event["requestContext"].get("connectionId")

    if event["requestContext"]["eventType"] == "CONNECT":
        log.info("Connect requested (CID: {})".format(connectionID))

        # Ensure connectionID is valid
        if not connectionID:
            log.error("Failed: connectionId value not set.")
            return _get_response(500, "Connect not successful.")

        # Add connectionID to the database
        table = dynamodb.Table("chat_connections")
        table.put_item(Item={"ConnectionId": connectionID})
        return _get_response(200, "Connect successful.")

    elif event["requestContext"]["eventType"] == "DISCONNECT":
        log.info("Disconnect requested (CID: {})".format(connectionID))

        # Ensure connectionID is set
        if not connectionID:
            log.error("Failed: connectionId value not set.")
            return _get_response(500, "connectionId value not set.")

        # Remove the connectionID from the database
        table = dynamodb.Table("chat_connections")
        delete_response = table.delete_item(Key={"ConnectionId": connectionID})
        log.info(f'delete_response: {delete_response}')
        log.info("Disconnect successful")
        return _get_response(200, "Disconnect successful.")

    else:
        log.error("Connection manager received unrecognized eventType '{}'"\
                .format(event["requestContext"]["eventType"]))
        return _get_response(500, "Unrecognized eventType.")


def _get_response(status_code, body):
    if not isinstance(body, str):
        body = json.dumps(body)
    return {"statusCode": status_code, "body": body}

