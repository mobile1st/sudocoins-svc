import boto3
import json
import re
import http.client
import uuid
from boto3.dynamodb.conditions import Key
from datetime import datetime
from util import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug(f'set_vote event{event}')
    query_params = event['queryStringParameters']
    art_id = query_params.get('artId')
    vote = query_params.get('vote')
    unique_id = query_params.get('id')
    ip = query_params.get('ip')

    art_votes_record = {
        "unique_id": unique_id,
        "art_id": art_id,
        "ip": ip
    }
    dynamodb.Table('art_votes').put_item(
        Item=art_votes_record
    )


    return