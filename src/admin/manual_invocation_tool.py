import boto3
import traffic_report_counter_store
from collections import Counter
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')


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
    profiles = profile_table.scan()['Items']
    for item in profiles:
        if item.get('signupDate') is None:
            print(f'skippedItem={item}')
            continue
        date = datetime.fromisoformat(item['signupDate'])
        status = 'Profile'
        traffic_report_counter_store.lambda_handler(get_request(date, status), None)


def fill_traffic_reports_table_from_transaction():
    transaction_table = dynamodb.Table('Transaction')
    transactions = transaction_table.scan()['Items']
    for item in transactions:
        if item.get('started') is None or item.get('status') is None:
            print(f'skippedItem={item}')
            continue
        date = datetime.fromisoformat(item['started'])
        status = item['status']
        revenue = item.get('revenue')
        traffic_report_counter_store.lambda_handler(get_request(date, status, revenue), None)


def get_request(date, status, rev=None):
    format_date = date.strftime('%Y-%m-%d')
    revenue = 0 if rev is None else Decimal(rev)
    return {
        'Records': [
            {
                'Sns': {
                    'Message': f'{{\"status\":\"{status}\",\"date\":\"{format_date}\",\"revenue\":{revenue}}}'
                }
            }
        ]
    }


def main():
    print_statuses()
    # fill_traffic_reports_table_from_profile()
    # fill_traffic_reports_table_from_transaction()


if __name__ == "__main__":
    main()
