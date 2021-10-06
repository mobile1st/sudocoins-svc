import boto3
from util import sudocoins_logger
import json
import statistics
from datetime import datetime, timedelta

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    collections = get_config()
    log.info("got config")

    time_series = str(datetime.utcnow().isoformat()).split('T')[0]
    time_list = [time_series]
    count = 6
    while count > 0:
        new_time = (datetime.utcnow() - timedelta(days=count)).isoformat().split('T')[0]
        time_list.append(new_time)
        count -= 1

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
            'ProjectionExpression': 'date, trades'
        }

        response = dynamodb.batch_get_item(RequestItems={'time_series': query})

        final_series[i] = {}
        final_series[i]['floor'] = []
        final_series[i]['median'] = []

        for row in response['Responses']['time_series']:
            final_series[i]['floor'].append({row['date'], min(row['trades'])})
            final_series[i]['median'].append({row['date'], statistics.median(row['trades'])})


    return


def get_config():
    return dynamodb.Table('Config').get_item(
        Key={'configKey': 'Leaderboard'}
    )['Item']['creators']