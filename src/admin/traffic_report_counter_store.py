import boto3
import json
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
start_statuses = {'Started'}
profile_statuses = {'Profile'}
complete_statuses = {'Complete', 'C', 'success'}
term_statuses = {'Blocked', 'Invalid', '', 'Overquota', 'Screen-out', 'No Project', 'failure', 'F', 'P'}
date_format = '%Y-%m-%d'


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    print(message)
    date = datetime.utcnow().strftime(date_format) if message.get('date') is None else message['date']

    traffic_reports_table = dynamodb.Table('TrafficReports')
    add_counter(traffic_reports_table, date, message)
    if 'status' in message and message['status'] in complete_statuses:
        if 'revenue' in message:
            increment_revenue(traffic_reports_table, date, message['revenue'])
        else:
            print('revenue is not present for message', json.dumps(message))


def add_counter(table, date, message):
    table.update_item(
        Key={'date': date},
        UpdateExpression='SET #attribute_name = if_not_exists(#attribute_name, :start) + :inc',
        ExpressionAttributeNames={
            '#attribute_name': get_attribute_name(message)
        },
        ExpressionAttributeValues={
            ':inc': 1,
            ':start': 0
        }
    )


def increment_revenue(table, date, revenue):
    table.update_item(
        Key={'date': date},
        UpdateExpression='SET revenue = if_not_exists(revenue, :start) + :inc',
        ExpressionAttributeValues={
            ':inc': Decimal(str(revenue)),  # str cast to avoid decimal.Inexact
            ':start': 0
        }
    )


# {'Blocked', 'Invalid', '', 'No Project', 'Overquota', 'Complete', 'Started', 'Screen-out'}
def get_attribute_name(message):
    has_status_property = 'status' in message
    if 'start' in message and message['start'] == 1:  # TODO: make uniform message
        return 'starts'
    if 'profile' in message and message['profile'] == 1:  # TODO: make uniform message
        return 'profiles'
    if has_status_property and message['status'] in complete_statuses:
        return 'completes'
    if has_status_property and message['status'] in term_statuses:
        return 'terms'
    if has_status_property and message['status'] in start_statuses:
        return 'starts'
    if has_status_property and message['status'] in profile_statuses:
        return 'profiles'
    raise Exception('cannot get attribute name for message ' + json.dumps(message))
