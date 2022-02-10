import boto3
from util import sudocoins_logger
import json
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
import os
import oauth2 as oauth
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')

    ifps = get_ipfs(art['collection_code'])

    if art['twitter'] is None:
        followers = "false"
    else:
        followers = call_twitter(art['twitter'])

    dynamodb.Table('collections').update_item(
        Key={'collection_id': art['collection_code']},
        UpdateExpression="SET followers=:tc, ipfs=:dec",
        ExpressionAttributeValues={
            ':tc': followers,
            ':dec': ifps
        }
    )

    log.info(" collection updated")

    return


def call_twitter(twitter_name):
    access_token = "1464273755510042624-t4pN0z7CPrwq6rbvR8qDfttLruRxwE"

    access_token_secret = "2hdTUXyCfjgGp8ObuKm6gMf7iX8cHCBJIsnGsW98iTKDY"
    consumer_key = "FZzJVAxaXk7kXEpev7GNs2AoN"
    consumer_secret = "8zIt0IMRDxHifySEQk8cqWzZVZSnAZaSqZ7StoEugoJcSpzvJ9"

    token = oauth.Token(access_token, access_token_secret)
    consumer = oauth.Consumer(consumer_key, consumer_secret)
    client = oauth.Client(consumer, token)

    request_uri = 'https://api.twitter.com/1.1/users/show.json?screen_name=' + twitter_name
    resp, content = client.request(request_uri, 'GET')

    response = json.loads(content)

    return response['followers_count']


def get_ipfs(collection_id):
    data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(collection_id),
        ScanIndexForward=False,
        Limit=1,
        IndexName='collection_id-event_date-index',
        ProjectionExpression='art_url'
    )

    art_url = data['Items'][0]['art_url']
    log.info(art_url)

    tmp = 'false'
    ipfs = ['ipfs', 'pinata', 'arweave']
    for i in ipfs:
        if i in art_url:
            tmp = 'true'
    log.info(tmp)
    return tmp



