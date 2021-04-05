import boto3
import json
import logging
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto

logging.getLogger().setLevel(logging.INFO)
dynamodb = boto3.resource('dynamodb')

complete_statuses = {'Complete', 'C', 'success'}
term_statuses = {'Blocked', 'Invalid', '', 'Overquota', 'Screen-out', 'No Project', 'failure', 'F', 'P', 'np'}

date_format = '%Y-%m-%d'


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    logging.debug(message)

    date = datetime.utcnow().strftime(date_format) if message.get('date') is None else message['date']
    traffic_reports_table = dynamodb.Table('TrafficReports')
    increment_counters(traffic_reports_table, date, message)


def increment_counters(table, date, message):
    attribute_name, report_type, report_sub_type = get_update_details(message)
    sort_key = f'{report_type.value}#{report_sub_type}'
    table.update_item(
        Key={
            'date': date,
            'reportType': sort_key
        },
        UpdateExpression='SET #attribute_name = if_not_exists(#attribute_name, :start) + :inc',
        ExpressionAttributeNames={
            '#attribute_name': attribute_name.value
        },
        ExpressionAttributeValues={
            ':inc': 1,
            ':start': 0
        }
    )
    if attribute_name is AttributeName.COMPLETES:
        if 'revenue' not in message:
            raise Exception(f'revenue is not present for message: {json.dumps(message)}')
        increment_revenue(table, date, sort_key, message['revenue'])


def increment_revenue(table, date, sort_key, revenue):
    table.update_item(
        Key={
            'date': date,
            'reportType': sort_key
        },
        UpdateExpression='SET revenue = if_not_exists(revenue, :start) + :inc',
        ExpressionAttributeValues={
            ':inc': Decimal(str(revenue)),  # str cast to avoid decimal.Inexact
            ':start': 0
        }
    )


def get_update_details(message):
    if 'source' not in message:
        raise Exception('could not determine flow, because source is missing from the message')
    event_source = EventSource(message['source'])
    if event_source is EventSource.PROFILE:
        return handle_profile_events(message)
    if event_source is EventSource.SURVEY_START:
        return handle_survey_start_events(message)
    if event_source is EventSource.SURVEY_END:
        return handle_survey_end_events(message)
    raise Exception(f'cannot get update details for message: {json.dumps(message)}')


def handle_profile_events(message):
    report_sub_type = 'unknown' if message.get('signUpMethod') is None else message['signUpMethod']
    return AttributeName.PROFILES, ReportType.PROFILE, report_sub_type


def handle_survey_start_events(message):
    if 'buyerName' not in message:
        raise Exception('buyerName is missing from the message')
    return AttributeName.STARTS, ReportType.BUYER, message['buyerName']


def handle_survey_end_events(message):
    if 'status' not in message:
        raise Exception('could not determine attribute name, because status is missing from the message')
    if 'buyerName' not in message:
        raise Exception('buyerName is missing from the message')
    status = message['status']
    attribute_name = None
    if status in complete_statuses:
        attribute_name = AttributeName.COMPLETES
    elif status in term_statuses:
        attribute_name = AttributeName.TERMS
    return attribute_name, ReportType.BUYER, message['buyerName']


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class EventSource(AutoName):
    PROFILE = auto()
    SURVEY_START = auto()
    SURVEY_END = auto()


class ReportType(AutoName):
    BUYER = auto()
    PROFILE = auto()
    CARD = auto()


class AttributeName(Enum):
    STARTS = 'starts'
    PROFILES = 'profiles'
    COMPLETES = 'completes'
    TERMS = 'terms'
