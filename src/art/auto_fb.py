import boto3
from util import sudocoins_logger
import urllib
from decimal import Decimal, getcontext
from datetime import datetime
import http.client
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
getcontext().prec = 18


def lambda_handler(event, context):
    page_id = 156939157825276
    access_token = 'EAAUWkjPPJusBADssPqnqfEiNcAEuiHYyRrLcspbkcKLHVVuEeUfDaaJZAoFPjcnx72z8EvAvqx5IZAge9iDUHAIqy3AZAOiVOXKq7fDg4kG2moreCZBFQRA3C1A6DjgXZCrCwSyreUtqk8nwRW4j9tmsvIY7iOJXIZB1GCQYifdbabEZBYNBt7u '
    msg, link = get_art()
    path = '/{0}/feed?message={1}&link={2}&access_token={3}'.format(page_id, msg, link, access_token)
    log.info(f'msg & link: {msg} {link}')
    '''
    conn = http.client.HTTPSConnection("https://graph.facebook.com")
    conn.request("POST", path)
    response = conn.getresponse()
    facebook_response = json.loads(response.read())
    log.info(f'facebook_response: {facebook_response}')
    '''
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
        if 'name' in art and 'name' in art['collection_data']:
            resp = dynamodb.Table('auto_tweet').get_item(
                Key={'art_id': i})
            if 'Item' in resp:
                continue
            message = art['name'] + " of the " + art['collection_data']['name'] + " collection sells for "
            usd_price = "${:,.2f}".format(round(((Decimal(art['last_sale_price']) / (10**18)) / eth_rate), 2))
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
