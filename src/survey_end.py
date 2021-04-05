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

        query = event["queryStringParameters"]
        hash_state = verify_hash(host_url, query)

        msg_value = {
            "referer": referer,
            "queryStringParameters": query,
            "hashState": hash_state,
            "buyerName": "cint"
        }
        enqueue(msg_value)

        token = status_to_token(query.get('status', '').lower(), hash_state)

        return create_redirect(redirect_url, token)

    except Exception as e:
        log.exception(e)
        return create_redirect('https://www.sudocoins.com/?', 'invalid')


def status_to_token(status, hash_state):
    if status == 'np' or status == 'p':  # no project or screen-out
        return 'P'
    elif status == 'bl':  # blocked
        return 'invalid'
    elif status == 's':  # security concern
        return 'invalid'
    elif status == 'oq':  # over quota
        return 'F'
    elif status == 't' or status == 'c':  # tentative or not tentative complete
        if not hash_state:
            log.warn('hash mismatch for complete returning invalid')
            return 'invalid'
        return 'C'

    log.warn(f'encountered unknown cint status={status}')
    return 'invalid'


def create_redirect(redirect_url, token):
    return {
        "statusCode": 302,
        "headers": {'Location': f'{redirect_url}msg={token}'},
        "body": '{}'
    }


def enqueue(msg):
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='EndTransaction')


def verify_hash(host_url, params):
    sha_url = params.get('h')
    if not sha_url:
        return False

    url = (host_url + "status={0}&sid={1}&tid={2}").format(params["status"], params["sid"], params["tid"])
    return check_hash(url, sha_url)


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
