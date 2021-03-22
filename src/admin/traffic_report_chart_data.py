import boto3
import collections
from decimal import Decimal
from datetime import datetime, timedelta, timezone

dynamodb = boto3.resource('dynamodb')
date_key_format = '%Y-%m-%d'
default_item = {'completes': Decimal('0'), 'profiles': Decimal('0'), 'revenue': Decimal('0'),
                'starts': Decimal('0'), 'terms': Decimal('0')}


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

    for index, key in enumerate(keys):
        date = key['date']
        epoch_date = to_epoch_millis(date)
        item = reports.get(date, default_item)

        daily_completes = int(item.get('completes', 0))
        daily_terms = int(item.get('terms', 0))
        daily_profiles = int(item.get('profiles', 0))
        daily_starts = int(item.get('starts', 0)) - daily_terms - daily_completes
        daily_revenue = int(item.get('revenue', 0) / 100)

        starts.append({'x': epoch_date, 'y': daily_starts})
        completes.append({'x': epoch_date, 'y': daily_completes})
        terms.append({'x': epoch_date, 'y': daily_terms})
        profiles.append({'x': epoch_date, 'y': daily_profiles})
        revenue.append({'x': epoch_date, 'y': daily_revenue})

        mva7_profiles_deque.append(daily_profiles)
        mva7_revenue_deque.append(daily_revenue)
        mva7_completes_deque.append(daily_completes)

        if index >= 6:
            mva7_profiles.append({'x': epoch_date, 'y': round(avg(mva7_profiles_deque))})
            mva7_revenue.append({'x': epoch_date, 'y': avg(mva7_revenue_deque)})
            mva7_completes.append({'x': epoch_date, 'y': avg(mva7_completes_deque)})

    return {
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


def avg(deque):
    total = 0
    for e in deque:
        total += e
    return total / len(deque)
