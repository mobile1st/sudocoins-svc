import json
import os
import boto3

dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')
config_table_name = os.environ["CONFIG_TABLE"]


def lambda_handler(event, context):
    print(event)

    try:
        redirect_url, expected_params = get_urls()

        if event["headers"] is None:
            referer = ""
        elif 'Referer' in event['headers']:
            referer = event["headers"]["Referer"]
        else:
            referer = ""

        if event["queryStringParameters"] is None:
            query_params = {}
            token = "invalid"
        else:
            query_params = event["queryStringParameters"]
            token = query_params.get('status')

        print(referer)

        msg_value = {
            "referer": referer,
            "queryStringParameters": query_params,
            "missingParams": get_missing_params(query_params, expected_params)
        }
        print(msg_value)

        enqueue_transaction_end_if_not_complete(query_params)

        return create_redirect(redirect_url, token)

    except Exception as e:
        print(e)
        return {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }


def create_redirect(redirect_url, token):
    data = {}
    response = {
        "statusCode": 302,
        "headers": {'Location': redirect_url + "msg=" + token},
        "body": json.dumps(data)
    }
    return response


def get_urls():
    config_table = dynamodb.Table(config_table_name)
    config_key = 'dynataRedirect'

    response = config_table.get_item(Key={'configKey': config_key})
    redirect_url = response['Item']["configValue"]["redirectUrl"]
    expected_params = response['Item']["configValue"]["expectedParams"]

    return redirect_url, expected_params


def get_missing_params(params, expected_params):
    received_params = params.keys()
    missing_params = []
    for i in expected_params:
        if i not in received_params:
            missing_params.append(i)
    return missing_params


def enqueue_transaction_end_if_not_complete(params):
    if params.get('status') == 'C':
        print(f"SKIP_COMPLETE_ON_REDIRECT {params}")  # redirects are insecure for completes
        return

    transaction_id = params.get('sub_id')
    if transaction_id:  # Dynata redirect has the transactionId in the sub_id parameter
        transaction_table = dynamodb.Table('Transaction')
        transaction = transaction_table.get_item(Key={'transactionId': transaction_id})['Item']
        if transaction.get('surveyCode') == params.get('status'):
            print(f"UP_TO_DATE {transaction_id}")
            return

        print(f"NEED_UPDATE {transaction_id}")

        user_id = params.get('userId')
        user_id = user_id[0: 36] if user_id else None
        msg = {
            'buyerName': 'dynata',
            'hashState': False,
            'queryStringParameters': params.copy()
        }
        msg['queryStringParameters']['endUserId'] = user_id

        queue = sqs.get_queue_by_name(QueueName='EndTransaction.fifo')
        queue.send_message(MessageBody=json.dumps(msg), MessageGroupId='EndTransaction')
