import sys
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

conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)


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

    return


def get_collections(period):
    results = []
    with conn.cursor() as cur:
        sql = "SELECT co.collection_code, price FROM nft.events ev inner join nft.collections coll on ev.collection_id = co.id where event_date >= now() - interval 1 %s and blockchain_id=1"
        sql2 = "SELECT co.collection_code, price FROM nft.events ev inner join nft.collections coll on ev.collection_id = co.id where event_date >= now() - interval 2 %s and event_date <= now() - interval 1 %s and blockchain_id=1"

        cur.execute(sql, period)
        result = cur.fetchall()
        results.append(result)
        cur.execute(sql2, (period, period))
        result2 = cur.fetchall()
        results.append(result2)
        conn.close()

    collections1 = {}
    for i in results[0]:
        if i[0] not in collections1:
            collections1[i[0]] = [i[1]]
        else:
            collections1[i[0]] = collections1[i[0]] = [i[1]] + [i[1]]

    collections2 = {}
    for i in results[0]:
        if i[0] not in collections2:
            collections2[i[0]] = [i[1]]
        else:
            collections2[i[0]] = collections2[i[0]] = [i[1]] + [i[1]]

    collection_medians1 = {}
    for i in collections1.keys():
        median = statistics.median(collections1[i])
        collection_medians1[i] = median

    collection_medians2 = {}
    for i in collections2.keys():
        median = statistics.median(collections2[i])
        collection_medians2[i] = median


    collection_list = []
    for i in collection_medians1:
        if i in collection_medians2:
            tmp = {
                "collection_id": i,
                "period2": collection_medians2[i],
                "period1": collection_medians1[i],
                "delta": Decimal((collection_medians1[i] - collection_medians2[i])/collection_medians2[i] * 100)
            }
            collection_list.append(tmp)

    #sort collection list so it's ranked high to low for median change.

    dynamodb = boto3.resource('dynamodb')

    key_list = []
    for i in collection_list:
        tmp = {
            "collection_id": i['collection_id']
        }

        key_list.append(tmp)

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, preview_url, collection_name, chart_data'
    }
    response = dynamodb.batch_get_item(RequestItems={'collections': query})

    for i in response['Responses']['collections']:
        for k in range(len(collection_list)):
            if i['collection_id'] == collection_list[k]['collection_id']:
                collection_list[k].update(i)
                continue

    return collection_list
