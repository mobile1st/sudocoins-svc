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

    open_sea_response = call_open_sea(created)
    count = 0
    for i in open_sea_response:
        try:
            open_sea_url = i.get('asset', {}).get('permalink', "")
            if open_sea_url.find('matic') != -1:
                msg = {
                    "blockchain": "Polygon",
                    "payment_token": i.get("payment_token"),
                    "event_type": i.get('event_type'),
                    "open_sea_url": i.get('asset', {}).get('permalink'),
                    "sale_price": i.get('total_price'),
                    "created_date": i.get('created_date', ""),
                    "asset": i.get('asset')
                }
            elif open_sea_url.find('klaytn') != -1:
                msg = {
                    "blockchain": "Klaytn",
                    "payment_token": i.get("payment_token"),
                    "event_type": i.get('event_type'),
                    "open_sea_url": i.get('asset', {}).get('permalink'),
                    "sale_price": i.get('total_price'),
                    "created_date": i.get('created_date', ""),
                    "asset": i.get('asset')
                }
            else:
                msg = {
                    "blockchain": "Ethereum",
                    "payment_token": i.get('payment_token'),
                    "event_type": i.get('event_type'),
                    "open_sea_url": i.get('asset', {}).get('permalink'),
                    "sale_price": i.get('total_price'),
                    "created_date": i.get('created_date', ""),
                    "asset": i.get('asset')
                }
            sns_client.publish(
                TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSeaTopic',
                MessageStructure='string',
                Message=json.dumps(msg)
            )
            log.info(f'art event published: {msg}')
            count += 1
            log.info(count)
        except Exception as e:
            log.info(f'art error: {e}')
            log.info(f'art event: {i}')
            count += 1
            log.info(count)

    return


def call_open_sea(created):
    path = "/api/v1/events?event_type=successful&only_opensea=false&offset=0&occurred_after="\
           + created + "&occurred_before=" + (datetime.fromisoformat(created) + timedelta(minutes=1)).isoformat()
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    response2 = response.read().decode('utf-8')
    open_sea_response = json.loads(response2)
    log.info(f'open_sea_response: {open_sea_response}')

    return open_sea_response['asset_events']



