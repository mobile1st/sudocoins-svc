import boto3
from util import sudocoins_logger
import statistics
from datetime import datetime, timedelta
from decimal import Decimal, getcontext

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    collections = get_config()
    log.info("got config")

    time_series = str(datetime.utcnow().isoformat()).split('T')[0]
    time_list = []
    count = 6
    while count > 0:
        new_time = (datetime.utcnow() - timedelta(days=count)).isoformat().split('T')[0]
        time_list.append(new_time)
        count -= 1
    time_list.append(time_series)
    print(time_list)

    final_series = {}
    for i in collections:
        keys_list = []
        collection_id = i['collection_id']
        for k in time_list:
            tmp = {
                "date": k,
                "collection_id": collection_id
            }
            keys_list.append(tmp)

        query = {
            'Keys': keys_list,
            'ProjectionExpression': '#d, trades',
            'ExpressionAttributeNames': {'#d': 'date'}
        }

        response = dynamodb.batch_get_item(RequestItems={'time_series': query})

        final_series[collection_id] = {}
        final_series[collection_id]['floor'] = []
        final_series[collection_id]['median'] = []

        getcontext().prec = 18

        floor1 = 0
        floor2 = 0
        for row in response['Responses']['time_series']:
            floor2 = Decimal(min(row['trades'])) / (10 ** 18)

            if floor2 > 0:
                final_series[collection_id]['floor'].insert(0, {"x": row['date'],
                                                                "y": Decimal(min(row['trades'])) / (10 ** 18)})
                floor1 = Decimal(min(row['trades'])) / (10 ** 18)
            else:
                if floor1 == 0:
                    continue
                else:
                    final_series[collection_id]['median'].insert(0, {"x": row['date'],
                                                                     "y": Decimal(statistics.median(row['trades'])) / (
                                                                             10 ** 18)})
                    continue
            final_series[collection_id]['median'].insert(0, {"x": row['date'],
                                                             "y": Decimal(statistics.median(row['trades'])) / (
                                                                     10 ** 18)})

    for k in final_series:
        floor_list = final_series[k]['floor']
        median_list = final_series[k]['median']

        new_floor_list = sorted(floor_list, key=lambda i: i['x'], reverse=False)
        new_median_list = sorted(median_list, key=lambda i: i['x'], reverse=False)

        final_series[k]['floor'] = new_floor_list
        final_series[k]['median'] = new_median_list

    log.info("config updated")
    log.info(type(final_series))

    result = set_config(final_series)

    return final_series


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'Leaderboard'}
    )['Item']['collections']


def set_config(final_series):
    config_table = dynamodb.Table('Config')

    result = config_table.update_item(
        Key={
            'configKey': 'time_series'
        },
        UpdateExpression="set data_points=:dp",
        ExpressionAttributeValues={
            ":dp": final_series
        },
        ReturnValues="ALL_NEW"
    )

    return result