import json

import boto3
import collections
import sudocoins_logger
from decimal import Decimal
from datetime import datetime, timedelta, timezone

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
date_key_format = '%Y-%m-%d'
default_item = {'buyer': {}, 'profiles': Decimal('0')}


def lambda_handler(event, context):
    keys = generate_input_keys()
    response = dynamodb.batch_get_item(
        RequestItems={
            'TrafficReports': {
                'Keys': keys
            }
        }
    )
    items = response['Responses']['TrafficReports']
    reports = {}
    for item in items:
        reports[item['date']] = item

    mva7_profiles_deque = collections.deque(maxlen=7)
    mva7_revenue_deque = collections.deque(maxlen=7)
    mva7_completes_deque = collections.deque(maxlen=7)
    mva7_profiles = []
    mva7_revenue = []
    mva7_completes = []
    starts = []
    completes = []
    terms = []
    profiles = []
    revenue = []

    filter_to_buyer = None if event.get('queryStringParameters') is None else event['queryStringParameters'].get('buyerName')
    buyers = {filter_to_buyer} if filter_to_buyer else set()

    log.debug(f'filter_to_buyer: {filter_to_buyer} event: {event}')

    for index, key in enumerate(keys):
        date = key['date']
        epoch_date = to_epoch_millis(date)
        item = reports.get(date, default_item.copy())
        buyer = item.get('buyer', {})

        daily_counters = DailyCounters()
        if filter_to_buyer:
            daily_counters.add(buyer.get(filter_to_buyer))
        else:
            for buyerName in buyer.keys():
                buyers.add(buyerName)
                daily_counters.add(buyer[buyerName])

        daily_profiles = int(item.get('profiles', 0))

        starts.append({'x': epoch_date, 'y': daily_counters.daily_starts})
        completes.append({'x': epoch_date, 'y': daily_counters.daily_completes})
        terms.append({'x': epoch_date, 'y': daily_counters.daily_terms})
        profiles.append({'x': epoch_date, 'y': daily_profiles})
        revenue.append({'x': epoch_date, 'y': daily_counters.daily_revenue})

        mva7_profiles_deque.append(daily_profiles)
        mva7_revenue_deque.append(daily_counters.daily_revenue)
        mva7_completes_deque.append(daily_counters.daily_completes)

        if index >= 6:
            mva7_profiles.append({'x': epoch_date, 'y': round(sum(mva7_profiles_deque) / len(mva7_profiles_deque))})
            mva7_revenue.append({'x': epoch_date, 'y': sum(mva7_revenue_deque) / len(mva7_revenue_deque)})
            mva7_completes.append({'x': epoch_date, 'y': sum(mva7_completes_deque) / len(mva7_completes_deque)})

    return {
        'buyers': list(buyers),
        'lastMva7': {
            'completes': mva7_completes[len(mva7_completes) - 2]['y'],
            'profiles': mva7_profiles[len(mva7_profiles) - 2]['y'],
            'revenue': mva7_revenue[len(mva7_revenue) - 2]['y']
        },
        'trafficChart': {
            'starts': starts,
            'completes': completes,
            'terms': terms,
            'mva7Completes': mva7_completes
        },
        'profileChart': {
            'profiles': profiles,
            'mva7': mva7_profiles
        },
        'revenueChart': {
            'revenue': revenue,
            'mva7': mva7_revenue
        }
    }


def generate_input_keys():  # this is controlling the flow, guarantees sorting
    result = []
    for i in reversed(range(90)):
        day = datetime.utcnow() - timedelta(days=i)
        key = day.strftime(date_key_format)
        result.append({'date': key})
    return result


def to_epoch_millis(date_string):
    return int(datetime.strptime(date_string, date_key_format).replace(tzinfo=timezone.utc).timestamp() * 1000)


class DailyCounters:
    daily_completes = 0
    daily_terms = 0
    daily_starts = 0
    daily_revenue = 0

    def add(self, buyer_counters):
        if buyer_counters is None:
            return
        buyer_daily_completes = int(buyer_counters.get('completes', 0))
        buyer_daily_terms = int(buyer_counters.get('terms', 0))
        self.daily_completes += buyer_daily_completes
        self.daily_terms += buyer_daily_terms
        self.daily_starts += max(int(buyer_counters.get('starts', 0)) - buyer_daily_terms - buyer_daily_completes, 0)
        self.daily_revenue += float(buyer_counters.get('revenue', 0) / 100)
