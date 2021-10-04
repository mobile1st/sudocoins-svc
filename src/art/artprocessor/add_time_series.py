import boto3
from util import sudocoins_logger
import json
import statistics

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    # art = event['Records'][0]['Sns']['Message']
    log.info(f'art: {art}')

    timestamp = art['event_date'].split('T')[0]
    collection_id = art['collection_id']
    lsp = art['last_sale_price']

    record = dynamodb.Table('time_series').get_item(Key={'date': timestamp, 'collection_id': collection_id})
    if 'Item' in record:
        trades = record['Item']['trades']
        trades.append(lsp)

        values = {
            ":tr": trades,
            ":me": statistics.median(trades),
            ":fl": min(trades),
            ":tc": len(trades),
            ":ce": max(trades)
        }

        dynamodb.Table('time_series').update_item(
            Key={'date': timestamp, 'collection_id': collection_id},
            UpdateExpression="set trades=:tr, median=:me, floor=:fl, trade_count=:tc, ceiling=:ce ",
            ExpressionAttributeValues=values,
            ReturnValues="ALL_NEW"
        )
        log.info(f'record updated')
        # . log.info(values)

    else:
        new_record = {
            "date": timestamp,
            "collection_id": collection_id,
            "median": lsp,
            "trades": [lsp],
            "floor": lsp,
            "trade_count": 1,
            "celing": lsp
        }
        dynamodb.Table('time_series').put_item(Item=new_record)
        log.info(f'record created')
        # . log.info(new_record)

    return