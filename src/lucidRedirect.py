import json
import boto3
from decimal import Decimal
import hmac
import hashlib
import base64
from urllib.parse import urlparse, parse_qs
from typing import AnyStr, Dict, List

sdc_redirect_invalid = 'https://www.sudocoins.com/invalid'
sdc_redirect_msg = 'https://www.sudocoins.com/?msg='


def lambda_handler(event, context):
    print(event)
    status = event['status']
    url = event['url']
    if status != 'failure' and status != 'success':
        print("Invalid status code on lucidRedirect=" + url)
        return {'redirect': sdc_redirect_invalid}

    parameters = parse_qs(urlparse(url).query)
    if not check_lucid_hash(url, value(parameters, 'hash')):
        print("Hash mismatch for lucidRedirect=" + url)
        return {'redirect': sdc_redirect_invalid}

    if status == 'failure':
        enqueue_end_transaction({
            "userId":   value(parameters, 'pid'),
            "transactionId": value(parameters, 'mid'),
            "hashState": 'true',
            "buyerName": "lucid",
            "status": status,
            "queryStringParameters": parameters
        })
        return {'redirect': sdc_redirect_msg + 'P'}

    # we have a complete
    enqueue_end_transaction({
        "userId": value(parameters, 'pid'),
        "transactionId": value(parameters, 'mid'),
        "surveyId": value(parameters, 'sur'),
        "revenue": value(parameters, 'c'),
        "surveyLoi": value(parameters, 'l'),
        "hashState": 'true',
        "buyerName": "lucid",
        "status": status,
        "queryStringParameters": parameters
        # "sudoCut": Decimal(value(parameters, 'c')) * Decimal('.7'),  # todo this looks off here
        # "userCut": (Decimal(value(parameters, 'c')) * Decimal('.7')) * Decimal('.8')  # todo this looks off here
    })

    return {'redirect': sdc_redirect_msg + 'C'}


def check_lucid_hash(url, expected_hash):
    key = 'bab' # os.environ["key"]
    hashed_url = url[:url.find('&hash=')]
    encoded_key = key.encode('utf-8')
    encoded_url = hashed_url.encode('utf-8')
    hashed = hmac.new(encoded_key, msg=encoded_url, digestmod=hashlib.sha1)
    digested_hash = hashed.digest()
    base64_encoded_result = base64.b64encode(digested_hash)
    calculated_hash = base64_encoded_result.decode('utf-8').replace('+', '-').replace('/', '_').replace('=', '')

    print(calculated_hash)
    return calculated_hash == expected_hash


def enqueue_end_transaction(msg):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
    return queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='EndTransaction')


def value(query: Dict[AnyStr, List[AnyStr]], parameter_name) -> AnyStr:
    """Gets the first value for a parameter name when present, otherwise returns None"""

    value_list = query.get(parameter_name)
    return None if (value_list is None or len(value_list) == 0) else value_list[0]

res = lambda_handler({'status': 'failure', 'url': 'https://www.sudocoins.com/lucid/success/?happy=2'}, None)
print('Result= ', res)