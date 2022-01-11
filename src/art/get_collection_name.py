import boto3
from util import sudocoins_logger
from art.art import Art
from boto3.dynamodb.conditions import Key
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
arts = Art(dynamodb)


def lambda_handler(event, context):
    set_log_context(event)
    body = json.loads(event['body'])

    collection_id = body.get('collection_id')
    collection_url = body.get('collection_url')


    if collection_id == "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270:jiometory-no-compute---":
        collection_id = "0xa7d8d9ef8d8ce8992df33d8b8cf4aebabd5bd270:jiometory-no-compute---ジオメトリ-ハ-ケイサンサレマセン-by-samsy"

    try:
        if collection_id is not None:

            res = dynamodb.Table('collections').get_item(
                Key={'collection_id': collection_id},
                ProjectionExpression="collection_data, sales_volume, collection_date, sale_count, floor, median, maximum, more_charts, open_sea, blockchain")

            if 'Item' in res and 'collection_data' in res['Item']:
                return res['Item']

        else:
            data = dynamodb.Table('collections').query(
                KeyConditionExpression=Key('collection_url').eq(collection_url),
                ScanIndexForward=False,
                Limit=1,
                IndexName='collection_url-index',
                ProjectionExpression='collection_data, sales_volume, collection_date, sale_count, floor, median, maximum, more_charts, open_sea, blockchain, summary_stats'
            )

            collection_data = data['Items'][0]

            return collection_data

    except Exception as e:
        log.info(f"status: failure - {e}")


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))

