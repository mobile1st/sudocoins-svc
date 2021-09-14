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

    created = dynamodb.Table('art').query(
        KeyConditionExpression=Key("sort_idx").eq('true') & Key("event_date").lt(time_now),
        ScanIndexForward=False,
        Limit=1,
        IndexName='sort_idx-event_date-index',
        ProjectionExpression="event_date"
    )['Items'][0]['event_date']

    log.info(f'created: {created}')

    difference = (datetime.fromisoformat(time_now) - datetime.fromisoformat(created)).seconds / 60
    log.info(f'difference: {difference}')
    if difference < 20:
        return

    open_sea_response = call_open_sea(created)
    count_eth = process_open_sea(open_sea_response)
    log.info(count_eth)

    while count_eth == 0:
        log.info("try again ")
        open_sea_response = call_open_sea((datetime.fromisoformat(created) + timedelta(minutes=3)).isoformat())
        count_eth = process_open_sea(open_sea_response)
        log.info(count_eth)

    return


def call_open_sea(created):
    end_time = (datetime.fromisoformat(created) + timedelta(minutes=3)).isoformat()
    path = "/api/v1/events?event_type=successful&only_opensea=false&offset=0&limit=300&occurred_after=" \
           + created + "&occurred_before=" + end_time
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)

    return open_sea_response['asset_events']


def process_open_sea(open_sea_response):
    count = 0
    count_eth = 0
    for i in open_sea_response:
        try:
            open_sea_url = i.get('asset', {}).get('permalink', "")
            if open_sea_url.find('matic') != -1:
                # log.info('matic')
                count += 1
                continue

            elif open_sea_url.find('klaytn') != -1:
                # log.info('klaytn')
                count += 1
                continue

            else:
                # log.info('ethereum')

                created_date = i.get('transaction')
                if created_date is not None:
                    created_date = created_date.get('timestemp', "")
                else:
                    created_date = i.get('created_date', "")

                msg = {
                    "blockchain": "Ethereum",
                    "payment_token": i.get('payment_token'),
                    "event_type": i.get('event_type'),
                    "open_sea_url": i.get('asset', {}).get('permalink'),
                    "sale_price": i.get('total_price'),
                    "created_date": created_date,
                    "asset": i.get('asset')
                }
                sns_client.publish(
                    TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSeaTopic',
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

    return count_eth



