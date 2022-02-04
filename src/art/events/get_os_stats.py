import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from decimal import Decimal
import time

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    data = dynamodb.Table('collections').query(
        KeyConditionExpression=Key('os_update').eq('false'),
        IndexName='os_update-index',
        ProjectionExpression='collection_id, open_sea'
    )

    collections = data['Items']

    while 'LastEvaluatedKey' in data:
        data = dynamodb.Table('collections').query(
            KeyConditionExpression=Key('os_update').eq('false'),
            IndexName='os_update-index',
            ProjectionExpression='collection_id, open_sea',
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        collections.extend(data['Items'])

    no_count = 0
    count = 0
    error = 0
    for i in collections:
        try:
            time.sleep(1)
            opensea_slug = i.get('open_sea')
            if opensea_slug is None:
                update_expression = "SET os_update=:lu"
                ex_att = {
                    ':lu': "true"
                }
                no_count += 1
                # log.info(f'numbers: {[no_count, count, error]}')
            else:
                try:
                    stats = call_open_sea(i.get('open_sea'))
                except:
                    error+=1
                    continue

                stats2 = get_stats(stats)

                #log.info(stats)
                update_expression = "SET os_update=:lu, open_sea_stats=:oss, percentage_total_owners=:pto"
                ex_att = {
                    ':lu': "true",
                    ':oss': stats2,
                    ':pto': stats2['percent_total_owners']
                }
                count += 1
                # log.info(f'numbers: {[no_count, count, error]}')

            res = dynamodb.Table('collections').update_item(
                Key={'collection_id': i['collection_id']},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=ex_att,
                ReturnValues="UPDATED_NEW"
            )

        except Exception as e:
            log.info(f'status - failure: {e}')
            error += 1
            #log.info(i)
            log.info(stats)

    log.info(f'no slug: {no_count}')
    log.info(f'count updated: {count}')
    log.info(f'error: {error}')

    return


def call_open_sea(slug):
    path = "/api/v1/collection/" + str(slug) + "/stats"
    # log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()
    decoded_response = response.read().decode('utf-8')
    open_sea_response = json.loads(decoded_response)
    # log.info(open_sea_response)

    return open_sea_response['stats']


def get_stats(stats):
    nft_count = stats.get('count')

    if nft_count == 0 or nft_count is None:
        percent_total_owners = 0
    else:
        percent_total_owners = (
                Decimal(str(stats['num_owners'])) / Decimal(str(stats['count'])) * 100).quantize(Decimal('1.00'))

    if stats['floor_price'] == 'None' or stats['floor_price'] is None:
        floor_price = 'None'
    else:
        floor_price = Decimal(str(stats['floor_price']))

    if stats['market_cap'] == 'None' or stats['market_cap'] is None:
        market_cap = 'None'
    else:
        market_cap = Decimal(str(stats['market_cap']))

    if stats['num_owners'] == 'None' or stats['num_owners'] is None:
        num_owners = 'None'
    else:
        num_owners = Decimal(str(stats['num_owners']))

    if stats['count'] == 'None' or stats['count'] is None:
        count = 'None'
    else:
        count = Decimal(str(stats['count']))

    if stats['total_volume'] == 'None' or stats['total_volume'] is None:
        total_volume = 'None'
    else:
        total_volume = Decimal(str(stats['total_volume']))

    if stats['total_sales'] == 'None' or stats['total_sales'] is None:
        total_sales = 'None'
    else:
        total_sales = Decimal(str(stats['total_sales']))

    if stats['seven_day_volume'] == 'None' or stats['seven_day_volume'] is None:
        week_volume = "None"
    else:
        week_volume = Decimal(str(stats['seven_day_volume']))

    stats = {
        "floor_price": floor_price,
        "market_cap": market_cap,
        "num_owners": num_owners,
        "count": count,
        'total_volume': total_volume,
        'total_sales': total_sales,
        '7d_volume': week_volume,
        'percent_total_owners': percent_total_owners

    }

    return stats