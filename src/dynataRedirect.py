import json
import os
import boto3

dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')


def lambda_handler(event, context):
    print(event)

    try:
        redirectUrl, expectedParams = getUrls()

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
            "missingParams": missingParams(query_params, expectedParams)
        }

        response = createRedirect(redirectUrl, token)

        print(msg_value)

        enqueue_transaction_end_if_not_complete(query_params)

        return response

    except Exception as e:
        print(e)
        response = {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }

        return response


def createRedirect(redirectUrl, token):
    data = {}
    response = {
        "statusCode": 302,
        "headers": {'Location': redirectUrl + "msg=" + token},
        "body": json.dumps(data)
    }
    return response


def getUrls():
    dynamodb = boto3.resource('dynamodb')
    configTableName = os.environ["CONFIG_TABLE"]
    configTable = dynamodb.Table(configTableName)
    configKey = 'dynataRedirect'

    response = configTable.get_item(Key={'configKey': configKey})
    redirectUrl = response['Item']["configValue"]["redirectUrl"]
    expectedParams = response['Item']["configValue"]["expectedParams"]

    return redirectUrl, expectedParams


def missingParams(params, expectedParams):
    receivedParams = params.keys()
    missingParams = []
    for i in expectedParams:
        if i not in receivedParams:
            missingParams.append(i)
    return missingParams


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

