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

    end_time = dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update']
    log.info(f'created: {end_time}')

    '''
    if created >= "2021-11-27T05:05:00":
        return
    '''
    # difference = (datetime.fromisoformat(time_now) - datetime.fromisoformat(created)).total_seconds() / 60
    # log.info(f'difference: {difference}')
    """
    if difference < 20:
        return
    """

    start_time = (datetime.fromisoformat(end_time) - timedelta(minutes=4)).isoformat()

    open_sea_response = call_open_sea(start_time, end_time)
    length_response = len(open_sea_response)
    if length_response == 300:
        log.info('split and call again')
        start_time_small = (datetime.fromisoformat(end_time) - timedelta(minutes=2)).isoformat()
        start_time = (datetime.fromisoformat(end_time) - timedelta(minutes=4)).isoformat()
        open_sea_response1 = call_open_sea(start_time_small, end_time)
        open_sea_response2 = call_open_sea(start_time, start_time_small)
        open_sea_response = open_sea_response1 + open_sea_response2

    count_eth = process_open_sea(open_sea_response)
    log.info(count_eth)
    set_config(start_time)
    log.info(f'end_time: {end_time}')
    log.info(f'start_time: {start_time}')

    return


def call_open_sea(created, end_time):
    path = "/api/v1/events?event_type=successful&only_opensea=false&offset=0&limit=300&occurred_after=" \
           + created + "&occurred_before=" + end_time
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()

    response2 = response.read().decode('utf-8')

    open_sea_response = json.loads(response2)

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


def process_open_sea(open_sea_response):
    count = 0
    count_eth = 0
    count_matic = 0
    for i in open_sea_response:
        if i.get('asset') is None:
            try:
                log.info(i)
                log.info("bundle")
                asset_bundle = i.get('asset_bundle', {}).get('assets', [])
                bundle_count = len(asset_bundle)
                price_per = int(i.get('total_price')) / bundle_count
                payment_token = i.get('payment_token')
                event_type = i.get('event_type')
                winner_account = i.get('winner_account', {}).get('address', "")
                if i.get('transaction', {}).get('from_account') is None:
                    seller = "unknown"
                elif i.get('transaction', {}).get('from_account', {}).get('address') is None:
                    seller = "unknown"
                else:
                    seller = i.get('transaction', {}).get('from_account', {}).get('address')

                for k in asset_bundle:
                    log.info(k)
                    open_sea_url = i.get('permalink', "")
                    if open_sea_url.find('matic') != -1:
                        created_date = i.get('transaction')
                        if created_date is not None:
                            created_date = created_date.get('timestamp')
                            if created_date is None:
                                created_date = i.get('created_date', "")
                        else:
                            created_date = i.get('created_date', "")

                        msg = {
                            "blockchain": "polygon",
                            "payment_token": payment_token,
                            "event_type": event_type,
                            "open_sea_url": k.get('permalink'),
                            "sale_price": price_per,
                            "created_date": created_date,
                            "asset": k,
                            "owner": winner_account,
                            "collection_date": k.get('collection', {}).get('created_date'),
                            "seller": seller
                        }

                        sns_client.publish(
                            TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSea2Topic',
                            MessageStructure='string',
                            Message=json.dumps(msg)
                        )
                        log.info("bundle asset published")

                        count_matic += 1
                        count += 1

                    elif open_sea_url.find('klaytn') != -1:
                        count += 1
                        continue

                    else:
                        created_date = i.get('transaction')
                        if created_date is not None:
                            created_date = created_date.get('timestamp')
                            if created_date is None:
                                created_date = i.get('created_date', "")
                        else:
                            created_date = i.get('created_date', "")

                        msg = {
                            "blockchain": "Ethereum",
                            "payment_token": payment_token,
                            "event_type": event_type,
                            "open_sea_url": k.get('permalink'),
                            "sale_price": price_per,
                            "created_date": created_date,
                            "asset": k,
                            "owner": winner_account,
                            "collection_date": k.get('collection', {}).get('created_date'),
                            "seller": seller
                        }
                        sns_client.publish(
                            TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSea2Topic',
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
                if open_sea_url.find('matic') != -1:
                    created_date = i.get('transaction')
                    if created_date is not None:
                        created_date = created_date.get('timestamp')
                        if created_date is None:
                            created_date = i.get('created_date', "")
                    else:
                        created_date = i.get('created_date', "")
                    msg = {
                        "blockchain": "polygon",
                        "payment_token": i.get('payment_token'),
                        "event_type": i.get('event_type'),
                        "open_sea_url": i.get('asset', {}).get('permalink'),
                        "sale_price": i.get('total_price'),
                        "created_date": created_date,
                        "asset": i.get('asset'),
                        "owner": i.get('winner_account', {}).get('address', ""),
                        "collection_date": i.get('asset', {}).get('collection', {}).get('created_date')
                    }
                    if i.get('seller') is None:
                        seller = "unknown"
                    else:
                        seller = i.get('seller', {}).get('address', "")
                    msg['seller'] = seller
                    sns_client.publish(
                        TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSea2Topic',
                        MessageStructure='string',
                        Message=json.dumps(msg)
                    )
                    count += 1
                    count_matic += 1

                elif open_sea_url.find('klaytn') != -1:
                    count += 1
                    continue
                else:
                    created_date = i.get('transaction')
                    if created_date is not None:
                        created_date = created_date.get('timestamp')
                        if created_date is None:
                            created_date = i.get('created_date', "")
                    else:
                        created_date = i.get('created_date', "")
                    msg = {
                        "blockchain": "Ethereum",
                        "payment_token": i.get('payment_token'),
                        "event_type": i.get('event_type'),
                        "open_sea_url": i.get('asset', {}).get('permalink'),
                        "sale_price": i.get('total_price'),
                        "created_date": created_date,
                        "asset": i.get('asset'),
                        "owner": i.get('winner_account', {}).get('address', ""),
                        "collection_date": i.get('asset', {}).get('collection', {}).get('created_date')
                    }
                    if i.get('seller') is None:
                        seller = "unknown"
                    else:
                        seller = i.get('seller', {}).get('address', "")
                    msg['seller'] = seller
                    sns_client.publish(
                        TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSea2Topic',
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



