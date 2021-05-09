import boto3
import json
from collections import Counter
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from util import sudocoins_logger
from admin import traffic_report_counter_store

dynamodb = boto3.resource('dynamodb')
log = sudocoins_logger.get()

""" THIS IS JUST A PLAYGROUND / SANDBOX """


def print_statuses():
    transaction_table = dynamodb.Table('Transaction')

    transactions = transaction_table.scan()
    transactions = transactions['Items']

    statuses = []
    for item in transactions:
        statuses.append(item['status'])
    print(set(statuses))
    print(Counter(statuses))
    # {'Blocked', 'Invalid', '', 'No Project', 'Overquota', 'Complete', 'Started', 'Screen-out'}
    # Counter({'Started': 2545, 'Screen-out': 575, 'Complete': 299, 'Invalid': 37, 'Blocked': 20, 'Overquota': 4, '': 3, 'No Project': 3})


def fill_traffic_reports_table_from_profile():
    profile_table = dynamodb.Table('Profile')
    profiles = []
    scan_kwargs = {}
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = profile_table.scan(**scan_kwargs)
        profiles.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    for item in profiles:
        if item.get('signupDate') is None:
            print(f'skippedItem={item}')
            continue
        date = datetime.fromisoformat(item['signupDate'])
        method = item.get('signupMethod')
        try:
            req = get_request(date, 'PROFILE', method=method)
            traffic_report_counter_store.lambda_handler(req, None)
        except Exception as e:
            log.exception(f'exception during saving item: {item}, cause: {e}')


def fill_traffic_reports_table_from_transaction():
    transaction_table = dynamodb.Table('Transaction')
    transactions = []
    scan_kwargs = {}
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = transaction_table.scan(**scan_kwargs)
        transactions.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    for item in transactions:
        tr_date = item.get('completed') if item.get('started') is None else item['started']
        if tr_date is None or item.get('status') is None:
            print(f'skippedItem={item}')
            continue
        date = datetime.fromisoformat(tr_date)
        status = item['status']
        buyer = 'peanutLabs' if item.get('buyer') is None else item['buyer']
        revenue = item.get('revenue')
        source = 'SURVEY_START' if status == 'Started' else 'SURVEY_END'
        try:
            req = get_request(date, source, state=status, buyer=buyer, rev=revenue)
            traffic_report_counter_store.lambda_handler(req, None)
        except Exception as e:
            log.exception(f'exception during saving item: {item}, cause: {e}')


def get_request(date, source, state=None, method=None, buyer=None, rev=None):
    format_date = date.strftime('%Y-%m-%d')
    status = 'null' if state is None else f'\"{state}\"'
    revenue = 0 if rev is None else Decimal(rev)
    sign_up_method = 'null' if (method is None or method == '') else f'\"{method}\"'
    buyer_name = 'null' if buyer is None else f'\"{buyer}\"'
    message_json = f'''{{
    \"status\":{status},
    \"source\":\"{source}\",
    \"date\":\"{format_date}\",
    \"signUpMethod\":{sign_up_method},
    \"buyerName\":{buyer_name},
    \"revenue\":{revenue}
    }}'''
    return {
        'Records': [
            {
                'Sns': {
                    'Message': message_json
                }
            }
        ]
    }


# {
#    'userId': userId,
#    'source': 'PROFILE',
#    'status': 'CREATED',
#    'awsRequestId': context['aws_request_id'],
#    'timestamp': created,
#    'signUpMethod': signupMethod
# }

# {
#    'userId': userId,
#    'source': 'SURVEY_START',
#    'status': 'Started',
#    'awsRequestId': context['aws_request_id'],
#    'timestamp': timestamp,
#    'transactionId': data.get('transactionId'),
#    'buyerName': params.get('buyerName'),
# }

# {
#    "source": 'SURVEY_END',
#    "status": pub_status,
#    "transactionId": transaction_id,
#    "timestamp": timestamp,
#    "buyerName": buyer_name,
#    "userId": user_id,
#    "revenue": str(revenue)  # Decimal is not JSON serializable
# }


def send_sns_message():
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn="arn:aws:sns:us-west-2:977566059069:transaction-event",
        MessageStructure='string',
        MessageAttributes={
            'source': {
                'DataType': 'String',
                'StringValue': 'PROFILE'
            }
        },
        Message=json.dumps({
            'userId': 'testUserId',
            'source': 'PROFILE',
            'status': 'CREATED',
            'awsRequestId': 'testRequestId',
            'timestamp': 'now',
            'signupMethod': 'facebook'
        })
    )


def play_with_enum():
    print(list(EventSource))
    print(EventSource.PROFILE)
    print(type(EventSource.PROFILE))
    event_source = EventSource('SURVEY_START')
    if event_source is EventSource.PROFILE:
        print('from profile')
    elif event_source is EventSource.SURVEY_START:
        print('from start')
    elif event_source is EventSource.SURVEY_END:
        print('from end')


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class EventSource(AutoName):
    PROFILE = auto()
    SURVEY_START = auto()
    SURVEY_END = auto()


def main():
    print('main')
    log.info('asd')
    traffic_report_counter_store.lambda_handler({'Records': [{'Sns': {'Message': '{"test":"imports"}'}}]}, None)
    # print_statuses()
    # fill_traffic_reports_table_from_profile()
    # fill_traffic_reports_table_from_transaction()


if __name__ == "__main__":
    main()
