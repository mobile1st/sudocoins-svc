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

    tweet = get_art()

    if tweet is None:
        return

    data = {'status': tweet}
    request_uri = 'https://api.twitter.com/1.1/statuses/update.json'
    resp, content = client.request(request_uri, 'POST', urllib.parse.urlencode(data))

    # . log.info(f"response:  {resp}")
    # . log.info(f"content:  {content}")

    return content


def get_art():
    url = "https://app.sudocoins.com/art/social/"
    trending_art = dynamodb.Table('Config').get_item(
        Key={'configKey': 'TrendingArt'}
    )['Item']['art']

    eth_rate = dynamodb.Table('Config').get_item(
        Key={'configKey': 'HomePage'}
    )['Item']['ethRate']

    count = 0
    art_list = []

    for i in trending_art:
        if count < 1:
            art = dynamodb.Table('art').get_item(
                Key={'art_id': i})['Item']
            resp = dynamodb.Table('auto_tweet').get_item(
                Key={'art_id': art['art_id']})
            if 'Item' in resp:
                continue
            try:
                if 'name' in art['collection_data'] and art['collection_data']['name'] is not None:
                    if art['collection_data']['name'] in ['Mutant Ape Yacht Club',
                                                          'Bored Ape Yacht Club, Bored Ape Kennel Club']:
                        name = art['collection_data']['name']
                        token_id = art['contractId#tokenId'].split('#')[1]
                        name_split = name.split()
                        hashtag = "#" + (name.replace(" ", ""))
                        message = name_split[0] + " " + name_split[1] + " " + token_id + " sells for "
                        usd_price = "${:,.2f}".format(
                            round(((Decimal(art['last_sale_price']) / (10 ** 18)) / eth_rate), 2))
                        tweet = message + usd_price + " #NFT " + art['buy_url']
                        msg = {
                            "art_id": i,
                            "message": tweet,
                            "timestamp": str(datetime.utcnow().isoformat()),
                            "platform": "twitter"
                        }
                        dynamodb.Table('auto_tweet').put_item(Item=msg)
                        # . log.info(f"msg:  {msg}")

                        return tweet
                count += 1
                art_list.append(art)

            except Exception as e:
                count += 1
                log.info(e)
                continue

    for i in art_list:
        # . log.info(i)
        try:
            if i['name'] is not None or i['collection_data']['name'] is not None:
                resp = dynamodb.Table('auto_tweet').get_item(
                    Key={'art_id': i['art_id']})
                if 'Item' in resp:
                    continue
                name = i['name'] if i['name'] is not None else i['collection_data']['name']
                collection_name = i['collection_data']['name']
                hashtag = "#" + (collection_name.replace(" ", ""))
                message = name + " sells for"
                #  of the " + art['collection_data']['name'] + " collection
                usd_price = "${:,.2f}".format(round(((Decimal(i['last_sale_price']) / (10 ** 18)) / eth_rate), 2))
                tweet = message + " " + usd_price + " #NFT " + art['buy_url']
                msg = {
                    "art_id": i['art_id'],
                    "message": tweet,
                    "timestamp": str(datetime.utcnow().isoformat()),
                    "platform": "twitter"
                }
                dynamodb.Table('auto_tweet').put_item(Item=msg)
                # . log.info(f"msg:  {msg}")

                return tweet
        except Exception as e:
            log.info(e)
            continue
