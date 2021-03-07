import boto3
from datetime import datetime, timedelta

dynamodb = boto3.resource('dynamodb')
date_key_format = '%Y-%m-%d'


def lambda_handler(event, context):
    response = dynamodb.batch_get_item(
        RequestItems={
            'TrafficReports': {
                'Keys': generate_input_keys()
            }
        }
    )
    reports = response['Responses']['TrafficReports']

    mva7_days = get_mva7_days()
    mva7_profiles = 0
    mva7_revenue = 0
    total_starts = 0
    total_completes = 0
    total_profiles = 0
    total_revenue = 0
    starts = []
    completes = []
    profiles = []
    revenue = []

    for i in reports:
        date = i['date']
        daily_starts = int(i.get('starts', 0))
        daily_completes = int(i.get('completes', 0))
        daily_profiles = int(i.get('profiles', 0))
        daily_revenue = int(i.get('revenue', 0))  # TODO review revenue type

        if date in mva7_days:
            mva7_profiles += daily_profiles
            mva7_revenue += daily_revenue

        total_starts += daily_starts
        total_completes += daily_completes
        total_profiles += daily_profiles
        total_revenue += daily_revenue

        starts.append({'x': date, 'y': daily_starts})
        completes.append({'x': date, 'y': daily_completes})
        profiles.append({'x': date, 'y': daily_profiles})
        revenue.append({'x': date, 'y': daily_revenue})

    starts.sort(key=lambda e: e['x'])
    completes.sort(key=lambda e: e['x'])
    profiles.sort(key=lambda e: e['x'])
    revenue.sort(key=lambda e: e['x'])

    return {
        'totals': {
            'starts': total_starts,
            'completes': total_completes,
            'profiles': total_profiles,
            'revenue': total_revenue
        },
        'trafficChart': {
            'starts': starts,
            'completes': completes
            # TODO handle terms, no projects
        },
        'profileChart': {
            'profiles': profiles,
            'mva7': (mva7_profiles / 7.0)
        },
        'revenueChart': {
            'revenue': revenue,
            'mva7': (mva7_revenue / 7.0)
        }
    }


def generate_input_keys():
    result = []
    for i in range(90):
        day = datetime.today() - timedelta(days=i)
        key = day.strftime(date_key_format)
        result.append({'date': key})
    return result


def get_mva7_days():
    result = []
    for i in range(7):
        day = datetime.today() - timedelta(days=i)
        result.append(day.strftime(date_key_format))
    return result
