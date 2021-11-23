import boto3
from util import sudocoins_logger
import json
import statistics
from datetime import datetime, timedelta
from decimal import Decimal, getcontext

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')

    timestamp = art['event_date'].split('T')[0]
    collection_id = art['collection_id']
    lsp = art['last_sale_price']
    art_id = art['art_id']

    if lsp == 0:
        log.info(f'sale price 0: {art_id}')
        return

    floor, median = update_mappings(collection_id, lsp, art_id)
    log.info('mappings updated')

    update_trades(timestamp, collection_id, lsp, art_id, floor, median)
    log.info('trades updated')

    return


def update_trades(timestamp, collection_id, lsp, art_id, floor, median):
    try:
        if floor != 0:
            response = dynamodb.Table('time_series').update_item(
                Key={'date': timestamp, 'collection_id': collection_id},
                UpdateExpression="SET trades = list_append(if_not_exists(trades, :empty_list), :i),  "
                                 "trade_count = if_not_exists(trade_count, :st) + :inc,"
                                 "sales_volume = if_not_exists(sales_volume, :st) + :k,"
                                 "floor = :fl, median=:me",
                ExpressionAttributeValues={
                    ':i': [lsp],
                    ':empty_list': [],
                    ':k': lsp,
                    ':st': 0,
                    ':inc': 1,
                    ':fl': floor,
                    ":me": median
                },
                ReturnValues="UPDATED_OLD"
            )

            if 'Attributes' in response and 'floor' in response['Attributes']:
                old_floor = str(response['Attributes']['floor'])
                log.info(f'old floor: {old_floor}')
                log.info(f'current floor: {floor}')

                if response['Attributes']['floor'] > floor:
                    update_collection(collection_id)

            else:
                update_collection(collection_id)



        else:
            response = dynamodb.Table('time_series').update_item(
                Key={'date': timestamp, 'collection_id': collection_id},
                UpdateExpression="SET trades = list_append(if_not_exists(trades, :empty_list), :i),  "
                                 "trade_count = if_not_exists(trade_count, :st) + :inc,"
                                 "sales_volume = if_not_exists(sales_volume, :st) + :k, median=:me",
                ExpressionAttributeValues={
                    ':i': [lsp],
                    ':empty_list': [],
                    ':k': lsp,
                    ':st': 0,
                    ':inc': 1,
                    ":me": median
                },
                ReturnValues="UPDATED_NEW"
            )

    except Exception as e:
        log.info(e)

    return


def update_mappings(collection_id, lsp, art_id):
    try:
        response = dynamodb.Table('time_series').update_item(
            Key={'date': 'last_sale_price', 'collection_id': collection_id},
            UpdateExpression="SET arts.#art = :k ",
            ExpressionAttributeValues={
                ':k': lsp
            },
            ExpressionAttributeNames={
                '#art': art_id
            },
            ReturnValues="ALL_NEW"
        )

        my_dict = response['Attributes']['arts']
        floor = min(my_dict.values())
        median = statistics.median(my_dict.values())

        log.info(f'floor: {floor}')
        log.info(f'median: {median}')

        return floor, median

    except Exception as e:
        log.info(e)
        if e.response['Error']['Code'] == "ValidationException":
            dynamodb.Table('time_series').update_item(
                Key={'date': 'last_sale_price', 'collection_id': collection_id},
                UpdateExpression="SET arts = if_not_exists(arts, :new_dict)",
                ExpressionAttributeValues={
                    ':new_dict': {}
                },
                ReturnValues="ALL_NEW"
            )

            response = dynamodb.Table('time_series').update_item(
                Key={'date': 'last_sale_price', 'collection_id': collection_id},
                UpdateExpression="SET arts.#art = :i",
                ExpressionAttributeValues={
                    ':i': lsp
                },
                ExpressionAttributeNames={
                    '#art': art_id
                },
                ReturnValues="ALL_NEW"
            )

            my_dict = response['Attributes']['arts']
            floor = min(my_dict.items(), key=lambda x: x[1])[1]
            median = statistics.median(my_dict.values())
            log.info(f'floor: {floor}')
            log.info(f'floor: {median}')

            return floor, median

        else:
            log.info("other error")

            floor = 0
            return floor


def update_collection(collection_id):
    time_series = str(datetime.utcnow().isoformat()).split('T')[0]
    time_list = []
    count = 6
    while count > 0:
        new_time = (datetime.utcnow() - timedelta(days=count)).isoformat().split('T')[0]
        time_list.append(new_time)
        count -= 1
    time_list.append(time_series)

    final_series = {}
    keys_list = []

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

    final_series['floor'] = []
    getcontext().prec = 18

    for row in response['Responses']['time_series']:
        final_series['floor'].insert(0, {"x": row['date'],
                                         "y": Decimal(min(row['trades'])) / (10 ** 18)})

    floor_list = final_series['floor']
    new_floor_list = sorted(floor_list, key=lambda i: i['x'], reverse=False)
    final_series['floor'] = new_floor_list

    dynamodb.Table('collections').update_item(
        Key={
            'collection_id': collection_id
        },
        UpdateExpression="SET chart_data = :cd",
        ExpressionAttributeValues={
            ':cd': final_series
        },
        ReturnValues="UPDATED_NEW"
    )

    log.info(f'collection table updated: {collection_id}')

    return





