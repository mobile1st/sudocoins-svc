import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    open_sea_response = call_open_sea()
    for i in open_sea_response:
        open_sea_url = i.get('asset', {}).get('permalink', "")
        if open_sea_url.find('matic') != -1:
            msg = {
                "blockchain": "Polygon",
                "payment_token": i.get("payment_token"),
                "event_type": i.get('event_type'),
                "open_sea_url": i.get('asset', {}).get('permalink'),
                "sale_price_token": i.get('total_price'),
                "created_date": i.get('created_date', ""),
                "asset": i.get('asset')
            }
        else:
            msg = {
                "blockchain": "Ethereum",
                "payment_token": i.get('payment_token'),
                "event_type": i.get('event_type'),
                "open_sea_url": i.get('asset', {}).get('permalink'),
                "sale_price_token": i.get('total_price'),
                "created_date": i.get('created_date',"")
            }
        sns_client.publish(
            TopicArn='arn:aws:sns:us-west-2:977566059069:IngestOpenSeaTopic',
            MessageStructure='string',
            Message=json.dumps(msg)
        )
        log.info(f'art event published: {msg}')

    return


def call_open_sea():
    path = "/api/v1/events?event_type=successful&only_opensea=false&offset=0&limit=20"
    conn = http.client.HTTPSConnection("api.opensea.io")
    conn.request("GET", path)
    response = conn.getresponse()
    open_sea_response = json.loads(response.read())
    log.info(f'open_sea_response: {open_sea_response}')

    return open_sea_response['asset_events']

