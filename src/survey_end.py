import json
import os
import boto3
import hashlib
import hmac
import sudocoins_logger

log = sudocoins_logger.get()
sqs = boto3.resource('sqs')
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')

    try:
        redirect_url, host_url = get_urls()
        referer = event['headers'].get('Referer', "")

        hash_state = verify_hash(host_url, event["queryStringParameters"])

        msg_value = {
            "referer": referer,
            "queryStringParameters": event["queryStringParameters"],
            "hashState": hash_state,
            "buyerName": "cint"
        }
        enqueue(msg_value)

        token = 'valid' if hash_state else 'invalid'

        return create_redirect(redirect_url, token)

    except Exception as e:
        log.exception(e)
        return create_redirect('https://www.sudocoins.com/?', 'invalid')


def create_redirect(redirect_url, token):
    return {
        "statusCode": 302,
        "headers": {'Location': f'{redirect_url}msg={token}'},
        "body": '{}'
    }


def enqueue(msgValue):
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    queue.send_message(MessageBody=json.dumps(msgValue), MessageGroupId='EndTransaction')


def verify_hash(hostUrl, params):
    shaUrl = params.get('h')
    if not shaUrl:
        return False

    url = (hostUrl + "status={0}&sid={1}&tid={2}").format(params["status"], params["sid"], params["tid"])
    return check_hash(url, shaUrl)


def get_urls():
    table = dynamodb.Table('Config')
    response = table.get_item(Key={'configKey': 'surveyEnd'})
    config = response['Item']['configValue']
    return config['redirectUrl'], config['hostUrl']


def check_hash(url, url_hash):
    secret = os.environ["keyId"]
    signature = hmac.new(
        secret.encode('utf-8'),
        url.encode('utf-8'),
        digestmod=hashlib.sha256
    ).hexdigest()

    return signature == url_hash

