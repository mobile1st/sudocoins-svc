import boto3
from util import sudocoins_logger
from datetime import datetime
import uuid
import json
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    body = json.loads(event['body'])
    """
    art_record = {
        "art_id": body.get("art_id")
    }
    """

    # dynamodb.Table('art').put_item(Item=art_record)

    return


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))



