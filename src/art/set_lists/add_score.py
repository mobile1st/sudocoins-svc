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
    '''
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')
    collection_id = art['collection_id']
    collection_code = art['collection_code']
    twitter = art['twitter']

    last_update = str(datetime.utcnow().isoformat())
    '''
    collection_url = 'boredapeyachtclub'

    get_ipfs(collection_url)

    '''
    content = call_twitter()


    try:
        collection_record = dynamodb.Table('collections').get_item(
            Key={'collection_id': collection_code}
        )

    except Exception as e:
        log.info(e)


    return {
        "followers": content['followers_count']
    }
    '''


def call_twitter():
    access_token = "1464273755510042624-t4pN0z7CPrwq6rbvR8qDfttLruRxwE"
    # SGMbZgiWk0oNjkoet4RKr3DPt

    access_token_secret = "2hdTUXyCfjgGp8ObuKm6gMf7iX8cHCBJIsnGsW98iTKDY"
    # fuQEFEzcYxoGzP32CkO7pbvtoMHdKZ8TZYa6W3TGbDiAacHdax
    consumer_key = "FZzJVAxaXk7kXEpev7GNs2AoN"
    consumer_secret = "8zIt0IMRDxHifySEQk8cqWzZVZSnAZaSqZ7StoEugoJcSpzvJ9"

    # bear AAAAAAAAAAAAAAAAAAAAAJQAWQEAAAAAAE1Pwau2RGMyP4cfQXeh1I%2F%2BEkc%3DUWG6rJuxsGeFOz2Tf0GmOTlEa4X3IOnUIFXwlCiJjxBX3FwDE1
    token = oauth.Token(access_token, access_token_secret)
    consumer = oauth.Consumer(consumer_key, consumer_secret)
    client = oauth.Client(consumer, token)

    # data = {'status': tweet}
    request_uri = 'https://api.twitter.com/1.1/users/show.json?screen_name=twitterdev'
    resp, content = client.request(request_uri, 'GET')

    response = json.loads(content)

    return response


def get_ipfs(collection_url):
    data = dynamodb.Table('collections').query(
        KeyConditionExpression=Key('collection_url').eq(collection_url),
        ScanIndexForward=False,
        Limit=1,
        IndexName='collection_url-index',
        ProjectionExpression='art_url'
    )

    art_url = data['Items'][0]

    log.info('art_url')



