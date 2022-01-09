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
    log.info(f'created: {start_time}')
    end_time = (datetime.fromisoformat(start_time) + timedelta(minutes=2)).isoformat()
    difference = (datetime.fromisoformat(time_now) - datetime.fromisoformat(start_time)).total_seconds() / 60
    log.info(f'difference: {difference}')
    if difference < 20:
        return

    #get created listings
    open_sea_response = call_open_sea(start_time, end_time, "created")

    length_response = len(open_sea_response)
    if length_response == 300:
        log.info('split and call again')
        middle_time = (datetime.fromisoformat(end_time) - timedelta(minutes=1)).isoformat()
        open_sea_response1 = call_open_sea(middle_time, end_time, "created")
        open_sea_response2 = call_open_sea(start_time, middle_time, "created")
        open_sea_response = open_sea_response1 + open_sea_response2

    count_eth = process_open_sea(open_sea_response)

    #get cancelled listings
    open_sea_response = call_open_sea(start_time, end_time, "cancelled")

    length_response = len(open_sea_response)
    if length_response == 300:
        log.info('split and call again')
        middle_time = (datetime.fromisoformat(end_time) - timedelta(minutes=1)).isoformat()
        open_sea_response1 = call_open_sea(middle_time, end_time, "cancelled")
        open_sea_response2 = call_open_sea(start_time, middle_time, "cancelled")
        open_sea_response = open_sea_response1 + open_sea_response2

    count_eth = process_open_sea(open_sea_response)




    log.info(count_eth)
    set_config(end_time)
    log.info(f'start_time: {start_time}')
    log.info(f'end_time: {end_time}')

    return


def call_open_sea(created, end_time, event_type):
    path = "/api/v1/events?event_type="+ event_type+ "&only_opensea=false&offset=0&limit=300&occurred_after=" \
           + created + "&occurred_before=" + end_time
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()
    # . log.info(f'response: {response}')

    decoded_response = response.read().decode('utf-8')
    # . log.info(f'response2: {response2}')

    open_sea_response = json.loads(decoded_response)

    return open_sea_response['asset_events']


def set_config(end_time):
    dynamodb.Table('Config').update_item(
        Key={
            'configKey': 'ingest2'
        },
        UpdateExpression="set last_update=:lu",
        ExpressionAttributeValues={
            ":lu": end_time
        },
        ReturnValues="ALL_NEW"
    )
    log.info("config updated")


def process_open_sea(open_sea_response):
    count = 0
    count_eth = 0
    count_matic = 0
    for i in open_sea_response:
        if i.get('asset') is None:
            try:
                # . log.info(i)
                log.info("bundle")

                listing_price = i.get('starting_price')
                listing_time = i.get('listing_time')
                if listing_time is None:
                    listing_time = i.get('created_date')

                asset_bundle = i.get('asset_bundle', {}).get('assets', [])
                bundle_count = len(asset_bundle)
                price_per = listing_price / bundle_count
                payment_token = i.get('payment_token')
                event_type = i.get('event_type')
                winner_account = i.get('winner_account', {}).get('address', "")

                for k in asset_bundle:
                    open_sea_url = i.get('permalink', "")

                    if open_sea_url.find('matic') != -1:
                        continue
                    elif open_sea_url.find('klaytn') != -1:
                        count += 1
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
                            "owner": winner_account,
                        }
                        sns_client.publish(
                            TopicArn='arn:aws:sns:us-west-2:977566059069:ListingsTopic',
                            MessageStructure='string',
                            Message=json.dumps(msg)
                        )
                        log.info("bundle asset published")

                        count += 1
                        count_eth += 1

            except Exception as e:
                log.info(e)
                count += 1
        else:
            try:
                open_sea_url = i.get('asset', {}).get('permalink', "")

                listing_price = i.get('starting_price')
                listing_time = i.get('listing_time')
                if listing_time is None:
                    listing_time = i.get('created_date')

                if open_sea_url.find('matic') != -1:
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
                        "owner": i.get('winner_account', {}).get('address', ""),

                    }

                    sns_client.publish(
                        TopicArn='arn:aws:sns:us-west-2:977566059069:ListingsTopic',
                        MessageStructure='string',
                        Message=json.dumps(msg)
                    )
                    count += 1
                    count_eth += 1

            except Exception as e:
                log.info(f'art error: {e}')
                log.info(f'art event: {i}')
                count += 1

    log.info(f'final count: {count}')
    log.info(f'eth count: {count_eth}')
    log.info(f'matic count: {count_matic}')

    return count_eth



