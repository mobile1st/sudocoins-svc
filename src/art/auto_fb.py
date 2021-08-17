import boto3
from util import sudocoins_logger
import urllib
from decimal import Decimal, getcontext
from datetime import datetime
import http.client
import json
import requests

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
getcontext().prec = 18


def lambda_handler(event, context):
    page_id = 156939157825276
    access_token = 'EAAUWkjPPJusBAOziGRR1Hv8mYgmdT5NnMogmo6ZBg5GVlr0Vxf1rsqJQ47fxUGOpR1VVMPrjuGZCti3hmqwVQMmxK3CDJhi5HayaZAmEUGFEp1qNJzNi8TcgdEXqLYDs02C2uwoIndBAjqS84SR9jO0vA0JReyuper56QciMj3O2BUTfTnn'
    msg, link = get_art()

    post_url = 'https://graph.facebook.com/{}/feed'.format(page_id)
    payload = {
        'message': msg,
        'link': link,
        'access_token': access_token
    }
    r = requests.post(post_url, data=payload)
    log.info(f'response: {r.text}')

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
                usd_price = "${:,.2f}".format(round(((Decimal(art['last_sale_price']) / (10 ** 18)) / eth_rate), 2))
                post = message + usd_price + " #NFT #Ethereum"
                link = url + art['art_id']
                msg = {
                    "art_id": i,
                    "message": post + " " + link,
                    "timestamp": str(datetime.utcnow().isoformat()),
                    "platform": "facebook"
                }
                dynamodb.Table('auto_tweet').put_item(Item=msg)
                log.info(f"msg:  {msg}")

                return post, link
        except Exception as e:
            log.info(e)
            continue
