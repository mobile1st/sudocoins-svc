import boto3
import json
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
complete_statuses = {'Complete'}
term_statuses = {'Blocked', 'Invalid', '', 'Overquota', 'Screen-out'}  # empty string???
no_project_statuses = {'No Project'}


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    print(message)

    traffic_reports_table = dynamodb.Table('TrafficReports')
    date = datetime.today().strftime('%Y-%m-%d')
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
            ':inc': revenue,
            ':start': 0
        }
    )


# {'Blocked', 'Invalid', '', 'No Project', 'Overquota', 'Complete', 'Started', 'Screen-out'}
def get_attribute_name(message):
    has_status_property = 'status' in message
    if 'start' in message and message['start'] == 1:
        return 'starts'
    if 'profile' in message and message['profile'] == 1:
        return 'profiles'
    if has_status_property and message['status'] in complete_statuses:
        return 'completes'
    if has_status_property and message['status'] in term_statuses:
        return 'terms'
    if has_status_property and message['status'] in no_project_statuses:
        return 'noProjects'
    raise Exception('cannot get attribute name for message ' + json.dumps(message))
