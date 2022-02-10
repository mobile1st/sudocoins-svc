import pymysql
import boto3
from util import sudocoins_logger
import os
from datetime import datetime

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
            'configKey': 'TradesDelta'
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

    return {
        "day": day,
        "hour": hour,
        "week": week
    }


def get_collections(period):
    dynamodb = boto3.resource('dynamodb')
    start_time = dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update']
    date_object = datetime.fromisoformat(start_time)
    log.info(f'created: {start_time}')
    with conn.cursor() as cur:
        sql = "SELECT distinct co.collection_code, t2.count2, t1.count1, round(((t1.count1-t2.count2)/t2.count2*100),1) AS delta FROM (SELECT collection_id, COUNT(*) AS count1 FROM nft.events where event_id=1 and event_date >= %s - interval 1 " + period + " GROUP BY collection_id) t1 INNER JOIN (SELECT collection_id, COUNT(*) AS count2 FROM nft.events where event_id=1 and event_date >= %s - interval 2 " + period + " and event_date <= %s - interval 1 " + period + "  GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id INNER JOIN nft.collections co on co.id=t1.collection_id where t2.count2 > 10 order by delta desc limit 50;"
        cur.execute(sql, (date_object, date_object, date_object))
        result = cur.fetchall()

    collection_list = []
    for i in result:
        tmp = {
            "collection_id": i[0],
            "yesterday": i[1],
            "today": i[2],
            "delta": i[3]
        }
        collection_list.append(tmp)

    dynamodb = boto3.resource('dynamodb')

    key_list = []
    for i in collection_list:
        tmp = {
            "collection_id": i['collection_id']
        }

        key_list.append(tmp)

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, preview_url, collection_name, collection_url, open_sea_stats, collection_data, collection_date, maximum, trades_delta, open_sea'
    }
    response = dynamodb.batch_get_item(RequestItems={'collections': query})

    for i in response['Responses']['collections']:
        for k in range(len(collection_list)):
            if i['collection_id'] == collection_list[k]['collection_id']:
                collection_list[k].update(i)
                continue

    return collection_list
