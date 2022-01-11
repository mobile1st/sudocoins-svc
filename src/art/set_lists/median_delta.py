from datetime import datetime
import pymysql
import boto3
from util import sudocoins_logger
import os
import statistics
from decimal import Decimal, getcontext

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')

rds_host = os.environ['db_host']
name = os.environ['db_user']
password = os.environ['db_pw']
db_name = os.environ['db_name']
port = 3306


def lambda_handler(event, context):
    hour = get_collections("hour")
    day = get_collections("day")
    week = get_collections("week")

    config_table = dynamodb.Table('Config')
    config_table.update_item(
        Key={
            'configKey': 'MedianDelta'
        },
        UpdateExpression="set #d=:d, #h=:h, #w=:w",
        ExpressionAttributeValues={
            ":d": day,
            ":h": hour,
            ":w": week

        },
        ReturnValues="ALL_NEW",
        ExpressionAttributeNames={'#d': 'day', '#h': 'hour', '#w': 'week'}
    )

    return hour


def get_collections(period):
    dynamodb = boto3.resource('dynamodb')
    start_time = dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update']
    date_object = datetime.fromisoformat(start_time)
    log.info(f'created: {start_time}')
    results = []
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)
    with conn.cursor() as cur:
        sql = "select co.collection_code, ev.price from nft.events ev inner join nft.collections co on ev.collection_id = co.id where event_date >= %s - interval 1 " + str(
            period) + " and ev.blockchain_id=1 and price>0;"
        sql2 = "select co.collection_code, ev.price from nft.events ev inner join nft.collections co on ev.collection_id = co.id where event_date >= %s - interval 2 " + str(
            period) + " and event_date <= %s - interval 1 " + period + " and ev.blockchain_id=1 and price>0;"

        cur.execute(sql, date_object)
        result = cur.fetchall()
        results.append(result)
        cur.execute(sql2, (date_object,date_object))
        result2 = cur.fetchall()
        results.append(result2)
        conn.close()

    collections1 = {}
    for i in results[0]:
        if i[0] not in collections1:
            collections1[i[0]] = [i[1]]
        else:
            collections1[i[0]] = collections1[i[0]] + [i[1]]
    log.info("here0")
    collections2 = {}
    for i in results[1]:
        if i[0] not in collections2:
            collections2[i[0]] = [i[1]]
        else:
            collections2[i[0]] = collections2[i[0]] + [i[1]]
    log.info("here")

    collection_medians1 = {}
    for i in collections1.keys():
        median = statistics.median(collections1[i])
        collection_medians1[i] = median
    log.info("here2")

    collection_medians2 = {}
    for i in collections2.keys():
        median = statistics.median(collections2[i])
        collection_medians2[i] = median
    log.info("here3")

    count = 0
    collection_list = []
    for i in collection_medians1:
        count += 1
        log.info(count)
        if i in collection_medians2:
            tmp = {
                "collection_id": i,
                "period2": collection_medians2[i],
                "period1": collection_medians1[i],
                "delta": Decimal((collection_medians1[i] - collection_medians2[i]) / collection_medians2[i] * 100)
            }
            collection_list.append(tmp)

    # sort collection list so it's ranked high to low for median change.
    new_list = sorted(collection_list, key=lambda i: i['delta'], reverse=True)[:50]
    log.info("here2")
    for i in new_list:
        log.info(i)

    dynamodb = boto3.resource('dynamodb')

    key_list = []

    collection_list = new_list

    for i in collection_list:
        tmp = {
            "collection_id": i['collection_id']
        }

        key_list.append(tmp)

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, preview_url, collection_name, chart_data, collection_url'
    }
    response = dynamodb.batch_get_item(RequestItems={'collections': query})

    for i in response['Responses']['collections']:
        for k in range(len(collection_list)):
            # . log.info(collection_list[k])
            if i['collection_id'] == collection_list[k]['collection_id']:
                collection_list[k].update(i)
                continue

    return collection_list


