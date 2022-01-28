import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    time_now = str(datetime.utcnow().isoformat())
    log.info(f'time_now: {time_now}')

    start_time = dynamodb.Table('Config').get_item(Key={'configKey': 'listings'})['Item']['last_update']
    # start_time = "2022-01-09T23:08:11.111111"
    log.info(f'created: {start_time}')

    end_time = (datetime.fromisoformat(start_time) + timedelta(seconds=90)).isoformat()
    difference = (datetime.fromisoformat(time_now) - datetime.fromisoformat(start_time)).total_seconds() / 60
    log.info(f'difference: {difference}')
    if difference < 20:
        return

    # get created listings
    offset = 0
    open_sea_response = call_open_sea(start_time, end_time, "created", offset)
    log.info(f'length of response: {len(open_sea_response)}')
    listings = open_sea_response
    length = len(open_sea_response)

    while length == 300:
        offset += length
        log.info(offset)
        path = "/api/v1/events?event_type=created&only_opensea=false&offset=" + str(
            offset) + "&limit=300&occurred_after=" + start_time + "&occurred_before=" + end_time
        conn = http.client.HTTPSConnection("api.opensea.io")
        api_key = {"X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"}
        conn.request("GET", path, headers=api_key)
        response = conn.getresponse()
        decoded_response = response.read().decode('utf-8')
        open_sea_response = json.loads(decoded_response).get('asset_events')
        if open_sea_response is None:
            length = 300
            offset -= 300
        else:
            length = len(open_sea_response)
            listings = open_sea_response + listings

    length_listings = len(listings)
    log.info(f'length of listings: {length_listings}')

    count_eth = process_open_sea(listings)

    log.info(f'eth count created: {count_eth}')

    # finished with created

    # get cancelled listings
    log.info("begin cancelled")
    offset = 0
    open_sea_response = call_open_sea(start_time, end_time, "cancelled", offset)
    log.info(f'length of response: {len(open_sea_response)}')
    listings = open_sea_response
    length = len(open_sea_response)

    while length == 300:
        offset += length
        log.info(offset)
        path = "/api/v1/events?event_type=created&only_opensea=false&offset=" + str(
            offset) + "&limit=300&occurred_after=" + start_time + "&occurred_before=" + end_time
        conn = http.client.HTTPSConnection("api.opensea.io")
        api_key = {"X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"}
        conn.request("GET", path, headers=api_key)
        response = conn.getresponse()
        decoded_response = response.read().decode('utf-8')
        open_sea_response = json.loads(decoded_response)['asset_events']
        if open_sea_response is None:
            length = 300
            offset -= 300
        else:
            length = len(open_sea_response)
            listings = open_sea_response + listings

    length_listings = len(listings)
    log.info(f'length of listings: {length_listings}')

    count_eth = process_open_sea(listings)
    log.info(f'eth count created: {count_eth}')

    # finished with created

    log.info(f'start_time: {start_time}')
    log.info(f'end_time: {end_time}')

    set_config(end_time)

    return


def call_open_sea(created, end_time, event_type, offset):
    path = "/api/v1/events?event_type=" + event_type + "&only_opensea=false&offset=" + str(
        offset) + "&limit=300&occurred_after=" \
           + created + "&occurred_before=" + end_time
    # log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()

    decoded_response = response.read().decode('utf-8')

    open_sea_response = json.loads(decoded_response)

    return open_sea_response['asset_events']


def process_open_sea(open_sea_response):
    count = 0
    count_eth = 0
    errors = 0
    for i in open_sea_response:
        try:
            if i.get('asset') is None:
                log.info("bundle")
                count += 1
                event_type = i.get('event_type')
                asset_bundle = i.get('asset_bundle', {}).get('assets', [])
                bundle_count = len(asset_bundle)
                if event_type == 'cancelled':
                    listing_price = None
                    price_per = None
                else:
                    listing_price = i.get('starting_price')
                    price_per = int(listing_price) / bundle_count
                listing_time = i.get('listing_time')
                if listing_time is None:
                    listing_time = i.get('created_date')

                payment_token = i.get('payment_token')

                owner = i.get('owner', {}).get("address")
                for k in asset_bundle:
                    open_sea_url = i.get('permalink', "")

                    if open_sea_url.find('matic') != -1:
                        continue
                    elif open_sea_url.find('klaytn') != -1:
                        continue

                    else:

                        msg = {
                            "blockchain": "Ethereum",
                            "payment_token": payment_token,
                            "event_type": event_type,
                            "open_sea_url": k.get('permalink'),
                            "listing_price": price_per,
                            "listing_time": listing_time,
                            "asset": k,
                            "owner": owner,
                            "bundle": "true",
                            "auction_type": k.get('auction_type')
                        }
                        sns_client.publish(
                            TopicArn='arn:aws:sns:us-west-2:977566059069:ListingsTopic',
                            MessageStructure='string',
                            Message=json.dumps(msg)
                        )
                        # log.info(msg)
                        # log.info(k)

                        count_eth += 1

            else:
                open_sea_url = i.get('asset', {}).get('permalink', "")
                listing_price = i.get('starting_price')
                listing_time = i.get('listing_time')
                if listing_time is None:
                    listing_time = i.get('created_date')

                if open_sea_url.find('matic') != -1:
                    count += 1
                    continue
                elif open_sea_url.find('klaytn') != -1:
                    count += 1
                    continue
                else:

                    msg = {
                        "blockchain": "Ethereum",
                        "payment_token": i.get('payment_token'),
                        "event_type": i.get('event_type'),
                        "open_sea_url": i.get('asset', {}).get('permalink'),
                        "listing_price": listing_price,
                        "listing_time": listing_time,
                        "asset": i.get('asset'),
                        "owner": i.get('asset', {}).get('owner', {}).get("address"),
                        "bundle": "false",
                        "auction_type": i.get('auction_type')

                    }

                    sns_client.publish(
                        TopicArn='arn:aws:sns:us-west-2:977566059069:ListingsTopic',
                        MessageStructure='string',
                        Message=json.dumps(msg)
                    )

                    count += 1
                    count_eth += 1
        except Exception as e:
            log.info(f'status - failure: {e}')
            log.info(f'nft: {i}')
            errors += 1

    return count_eth


def set_config(end_time):
    dynamodb.Table('Config').update_item(
        Key={
            'configKey': 'listings'
        },
        UpdateExpression="set last_update=:lu",
        ExpressionAttributeValues={
            ":lu": end_time
        },
        ReturnValues="ALL_NEW"
    )
    log.info("config updated")