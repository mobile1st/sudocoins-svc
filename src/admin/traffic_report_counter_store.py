import boto3
import json
# from util import sudocoins_logger
from datetime import datetime
from decimal import Decimal
from enum import Enum, auto
from botocore.exceptions import ClientError

# log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')

complete_statuses = {'Complete', 'C', 'success', 'c', 't'}
term_statuses = {'Blocked', 'Invalid', '', 'Overquota', 'Screen-out', 'No Project',
                 'failure', 'invalid', 'F', 'P', 'np', 'N', 'p', 'bl', 's', 'oq'}

date_format = '%Y-%m-%d'
default_buyer_level_attributes = {'starts': 0, 'terms': 0, 'completes': 0, 'revenue': 0}


def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message'])
    # log.debug(message)

    date = datetime.utcnow().strftime(date_format) if message.get('date') is None else message['date']
    increment_counters(date, message)


def increment_counters(date, message):
    counter_name_enum, buyer = get_update_details(message)
    if counter_name_enum is None:
        raise Exception(f'could not determine counter name from status field {message}')
    counter_name = counter_name_enum.value
    table = dynamodb.Table('TrafficReports')
    if buyer:
        try:
            increment_buyer_level_counter(table, date, buyer, counter_name)
        except ClientError as e:
            # creating new top level attribute with nested properties the previous call failed
            if e.response['Error']['Code'] == 'ValidationException':
                insert_default_structure_if_not_exists(table, date, buyer)
                increment_buyer_level_counter(table, date, buyer, counter_name)
        if counter_name_enum is CounterName.COMPLETES:
            if 'revenue' not in message:
                raise Exception(f'revenue is not present for message: {json.dumps(message)}')
            # str cast to avoid decimal.Inexact
            increment_buyer_level_counter(table, date, buyer, 'revenue', Decimal(str(message['revenue'])))
    else:
        increment_attribute_level_counter(table, date, counter_name)


def insert_default_structure_if_not_exists(table, date, buyer):
    try:
        table.update_item(
            Key={'date': date},
            UpdateExpression='SET buyer = :empty_map',
            ConditionExpression='attribute_not_exists(buyer)',
            ExpressionAttributeValues={':empty_map': {}}
        )
    except ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise
    try:
        table.update_item(
            Key={'date': date},
            UpdateExpression='SET buyer.#buyer_name = :default',
            ConditionExpression='attribute_not_exists(buyer.#buyer_name)',
            ExpressionAttributeNames={'#buyer_name': buyer},
            ExpressionAttributeValues={':default': default_buyer_level_attributes}
        )
    except ClientError as e:
        # Ignore the ConditionalCheckFailedException, bubble up other exceptions.
        if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
            raise


def increment_buyer_level_counter(table, date, buyer_name, counter_name, inc_value=1):
    table.update_item(
        Key={'date': date},
        UpdateExpression='SET buyer.#buyer_name.#counter_name = buyer.#buyer_name.#counter_name + :inc',
        ExpressionAttributeNames={
            '#buyer_name': buyer_name,
            '#counter_name': counter_name,
        },
        ExpressionAttributeValues={':inc': inc_value}
    )


def increment_attribute_level_counter(table, date, attribute_name, inc_value=1):
    table.update_item(
        Key={'date': date},
        UpdateExpression='SET #attribute_name = if_not_exists(#attribute_name, :start) + :inc',
        ExpressionAttributeNames={
            '#attribute_name': attribute_name
        },
        ExpressionAttributeValues={
            ':inc': inc_value,
            ':start': 0
        }
    )


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
        return handle_profile_events()
    if event_source is EventSource.SURVEY_START:
        return handle_survey_start_events(message)
    if event_source is EventSource.SURVEY_END:
        return handle_survey_end_events(message)
    raise Exception(f'cannot get update details for message: {json.dumps(message)}')


def handle_profile_events():
    return CounterName.PROFILES, None


def handle_survey_start_events(message):
    if 'buyerName' not in message:
        raise Exception('buyerName is missing from the message')
    return CounterName.STARTS, message['buyerName']


def handle_survey_end_events(message):
    if 'status' not in message:
        raise Exception('could not determine counter name, because status is missing from the message')
    if 'buyerName' not in message:
        raise Exception('buyerName is missing from the message')
    status = message['status']
    counter_name = None
    if status in complete_statuses:
        counter_name = CounterName.COMPLETES
    elif status in term_statuses:
        counter_name = CounterName.TERMS
    return counter_name, message['buyerName']


class AutoName(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name


class EventSource(AutoName):
    PROFILE = auto()
    SURVEY_START = auto()
    SURVEY_END = auto()


class CounterName(Enum):
    STARTS = 'starts'
    PROFILES = 'profiles'
    COMPLETES = 'completes'
    TERMS = 'terms'
