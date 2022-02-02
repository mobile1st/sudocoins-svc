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
                stats = call_open_sea(i.get('open_sea'))
                nft_count = stats.get('count')
                if nft_count == 0 or nft_count is None:
                    percent_total_owners = 0
                else:
                    percent_total_owners = (
                                Decimal(str(stats['num_owners'])) / Decimal(str(stats['count'])) * 100).quantize(
                        Decimal('1.00'))

                stats = {
                    "floor_price": Decimal(str(stats['floor_price'])),
                    "market_cap": Decimal(str(stats['market_cap'])),
                    "num_owners": Decimal(str(stats['num_owners'])),
                    "count": Decimal(str(stats['count'])),
                    'total_volume': Decimal(str(stats['total_volume'])),
                    'total_sales': Decimal(str(stats['total_sales'])),
                    '7d_volume': Decimal(str(stats['seven_day_volume'])),
                    'percent_total_owners': percent_total_owners

                }
                update_expression = "SET os_update=:lu, open_sea_stats=:oss"
                ex_att = {
                    ':lu': "true",
                    ':oss': stats
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
            # log.info(f'numbers: {[no_count, count, error]}')
            log.info(i)

    # log.info(f'no slug: {no_count}')
    # log.info(f'count updated: {count}')
    # log.info(f'error: {error}')

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
