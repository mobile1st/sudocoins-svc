import boto3
from util import sudocoins_logger
import urllib
import oauth2 as oauth
from decimal import Decimal, getcontext
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
getcontext().prec = 18


def lambda_handler(event, context):
    access_token = "1341826225405308928-D3tZFaQMVnpMRqMFBxN4HeH7D2jfqb"
    access_token_secret = "aHVw47EV5W8U6L8cEmTi7QCGuOchsgA1bY63SkFlnTi8m"
    consumer_key = "zcmikK0KTZB7mkdVB2cM9SwJX"
    consumer_secret = "t3ss4GZgr38IvTzHZWfCivrlVyhKEfpZz5VvpP7UmvubIpZkUR"
    token = oauth.Token(access_token, access_token_secret)
    consumer = oauth.Consumer(consumer_key, consumer_secret)
    client = oauth.Client(consumer, token)

    tweet = get_art()

    data = {'status': tweet}
    request_uri = 'https://api.twitter.com/1.1/statuses/update.json'
    resp, content = client.request(request_uri, 'POST', urllib.parse.urlencode(data))

    log.info(f"response:  {resp}")
    log.info(f"content:  {content}")

    return


def get_art():
    url = "https://app.sudocoins.com/art/social/"
    trending_art = dynamodb.Table('Config').get_item(
        Key={'configKey': 'TrendingArt'}
    )['Item']['art']

    eth_rate = dynamodb.Table('Config').get_item(
        Key={'configKey': 'HomePage'}
    )['Item']['ethRate']

    for i in trending_art:
        log.info(i)
        art = dynamodb.Table('art').get_item(
            Key={'art_id': i})['Item']
        log.info(art)
        try:
            if 'name' is not None and art['collection_data']['name'] is not None:
                resp = dynamodb.Table('auto_tweet').get_item(
                    Key={'art_id': i})
                if 'Item' in resp:
                    continue
                message = art['name'] + " of the " + art['collection_data']['name'] + " collection sells for "
                usd_price = "${:,.2f}".format(round(((Decimal(art['last_sale_price']) / (10**18)) / eth_rate), 2))
                tweet = message + usd_price + " " + url + art['art_id'] + " #NFTs #ETH"
                msg = {
                    "art_id": i,
                    "message": tweet,
                    "timestamp": str(datetime.utcnow().isoformat()),
                    "platform": "twitter"
                }
                dynamodb.Table('auto_tweet').put_item(Item=msg)
                log.info(f"msg:  {msg}")

                return tweet
        except Exception as e:
            log.info(e)
            continue
