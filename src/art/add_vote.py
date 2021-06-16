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
    timestamp = query_params.get('timestamp')

    art_votes_record = {
        "unique_id": unique_id,
        "art_id": art_id,
        "ip": ip,
        "vote": vote
    }
    dynamodb.Table('art_votes').put_item(
        Item=art_votes_record
    )

    recent = get_recent(20, timestamp)

    vote = get_votes()

    for i in recent:

        continue

    return None


def get_recent(count, timestamp):
    return dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("recent_sk").lt(timestamp),
        ScanIndexForward=False,
        Limit=count,
        IndexName='Recent_index',
        ProjectionExpression="art_id, preview_url, art_url, #n, click_count, recent_sk",
        ExpressionAttributeNames={'#n': 'name'}
    )['Items']


def get_votes(unique_id):
    return dynamodb.Table('art_votes').query(
        KeyConditionExpression=Key("unique_id").eq(unique_id)
    )['Items']
