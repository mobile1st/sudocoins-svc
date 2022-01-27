import boto3
from util import sudocoins_logger
import http.client
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
from decimal import Decimal

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):

    time_now = str(datetime.utcnow().isoformat())
    period = (datetime.fromisoformat(time_now) - timedelta(hours=1)).isoformat()
    data = dynamodb.Table('collections').query(
        KeyConditionExpression=Key('last_update').lt(period),
        IndexName='last_update-index',
        ProjectionExpression='collection_id, open_sea'
    )

    collections = data['Items']
    while 'LastEvaluatedKey' in data:
        data = dynamodb.Table('collections').query(
            KeyConditionExpression=Key('last_update').lt(period),
            IndexName='last_update-index',
            ProjectionExpression='collection_id, open_sea',
            ExclusiveStartKey=data['LastEvaluatedKey']
        )
        collections.extend(data['Items'])

    count = 0
    for i in collections:
        try:
            stats = call_open_sea(i['open_sea'])
            stats = {
                "floor_price": str(stats['floor_price']),
                "market_cap": str(stats['market_cap']),
                "num_owners": str(stats['num_owners']),
                "count": str(stats['count']),
                'total_volume': str(stats['total_volume']),
                'total_sales': str(stats['total_sales']),
                '7d_volume': str(stats['seven_day_volume']),
                'percent_total_owners': (Decimal(str(stats['num_owners'])) / Decimal(str(stats['count'])) * 100).quantize(Decimal('1.00'))

            }
            update_expression = "SET last_update=:lu, open_sea_stats=:oss"
            ex_att = {
                ':lu': "true",
                ':oss': stats
            }
            res = dynamodb.Table('collections').update_item(
                Key={'collection_id': i['collection_id']},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=ex_att,
                ReturnValues="UPDATED_NEW"
            )
            log.info('data added to collection table')
            log.info(res)
            count += 1
            log.info(count)
        except Exception as e:
            log.info(f'status - failure: {e}')

    return


def call_open_sea(slug):
    path = "/api/v1/collection/" + slug + "/stats"
    log.info(f'path: {path}')
    conn = http.client.HTTPSConnection("api.opensea.io")
    api_key = {
        "X-API-KEY": "4714cd73a39041bf9cffda161163f8a5"
    }
    conn.request("GET", path, headers=api_key)
    response = conn.getresponse()
    decoded_response = response.read().decode('utf-8')
    open_sea_response = json.loads(decoded_response)
    log.info(open_sea_response)

    return open_sea_response['stats']




