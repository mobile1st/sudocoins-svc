import boto3
import collections
from decimal import Decimal
from datetime import datetime, timedelta, timezone

dynamodb = boto3.resource('dynamodb')
date_key_format = '%Y-%m-%d'
default_item = {'buyer': {}, 'profiles': Decimal('0')}


def lambda_handler(event, context):
    print(event)
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

    all_buyers = set()
    for r in reports.values():
        all_buyers.update(r.get('buyer', {}).keys())

    total_report = BuyerReport()
    buyer_reports = {}
    for buyer_key in all_buyers:
        buyer_reports[buyer_key] = BuyerReport()

    total_profiles_mva7_deque = collections.deque(maxlen=7)
    total_profiles_mva7 = []
    total_profiles_daily = []

    for index, key in enumerate(keys):
        date = key['date']
        epoch_date = to_epoch_millis(date)
        item = reports.get(date, default_item.copy())
        buyer = item.get('buyer', {})

        total_report.add(buyer.values(), epoch_date)
        for buyer_key in all_buyers:
            buyer_reports[buyer_key].add([buyer.get(buyer_key)], epoch_date)

        daily_profiles = int(item.get('profiles', 0))
        total_profiles_daily.append({'x': epoch_date, 'y': daily_profiles})
        total_profiles_mva7_deque.append(daily_profiles)
        if len(total_profiles_mva7_deque) == 7:
            total_profiles_mva7.append({'x': epoch_date, 'y': round(sum(total_profiles_mva7_deque) / len(total_profiles_mva7_deque))})

    result = {
        'buyers': sorted(all_buyers),
        'total': {
            'totals': {
                'completes': total_report.mva7_completes[len(total_report.mva7_completes) - 2]['y'],
                'profiles': total_profiles_mva7[len(total_profiles_mva7) - 2]['y'],
                'revenue': total_report.mva7_revenue[len(total_report.mva7_revenue) - 2]['y']
            },
            'traffic': {
                'starts': total_report.starts,
                'completes': total_report.completes,
                'terms': total_report.terms
            },
            'revenue': {
                'daily': total_report.revenue,
                'mva7': total_report.mva7_revenue
            },
            'profiles': {
                'daily': total_profiles_daily,
                'mva7': total_profiles_mva7
            }
        },
    }

    for buyer, report in buyer_reports.items():
        result.update({
            buyer: {
                'totals': {
                    'completes': report.mva7_completes[len(report.mva7_completes) - 2]['y'],
                    'revenue': report.mva7_revenue[len(report.mva7_revenue) - 2]['y']
                },
                'traffic': {
                    'starts': report.starts,
                    'completes': report.completes,
                    'terms': report.terms
                },
                'revenue': {
                    'daily': report.revenue,
                    'mva7': report.mva7_revenue
                }
            }
        })

    return result


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
    def __init__(self):
        self.daily_completes = 0
        self.daily_terms = 0
        self.daily_starts = 0
        self.daily_revenue = 0

    def add(self, buyer_counters):
        if buyer_counters is None:
            return
        buyer_daily_completes = int(buyer_counters.get('completes', 0))
        buyer_daily_terms = int(buyer_counters.get('terms', 0))
        self.daily_completes += buyer_daily_completes
        self.daily_terms += buyer_daily_terms
        self.daily_starts += max(int(buyer_counters.get('starts', 0)) - buyer_daily_terms - buyer_daily_completes, 0)
        self.daily_revenue += float(buyer_counters.get('revenue', 0) / 100)


class BuyerReport:
    def __init__(self):
        self.mva7_revenue_deque = collections.deque(maxlen=7)
        self.mva7_completes_deque = collections.deque(maxlen=7)
        self.mva7_revenue = []
        self.mva7_completes = []
        self.starts = []
        self.completes = []
        self.terms = []
        self.revenue = []

    def add(self, buyers, epoch_date):
        daily_counters = DailyCounters()
        for buyer in buyers:
            daily_counters.add(buyer)

        self.starts.append({'x': epoch_date, 'y': daily_counters.daily_starts})
        self.completes.append({'x': epoch_date, 'y': daily_counters.daily_completes})
        self.terms.append({'x': epoch_date, 'y': daily_counters.daily_terms})
        self.revenue.append({'x': epoch_date, 'y': daily_counters.daily_revenue})

        self.mva7_revenue_deque.append(daily_counters.daily_revenue)
        self.mva7_completes_deque.append(daily_counters.daily_completes)

        if len(self.mva7_revenue_deque) == 7:
            self.mva7_revenue.append({'x': epoch_date, 'y': sum(self.mva7_revenue_deque) / len(self.mva7_revenue_deque)})
            self.mva7_completes.append({'x': epoch_date, 'y': sum(self.mva7_completes_deque) / len(self.mva7_completes_deque)})
