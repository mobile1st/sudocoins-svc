import boto3
from util import sudocoins_logger
import statistics
import pymysql
import os
from datetime import datetime
import numpy as np
import robustats

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

rds_host = os.environ['db_host']
name = os.environ['db_user']
password = os.environ['db_pw']
db_name = os.environ['db_name']
port = 3306


def lambda_handler(event, context):

    event_date = datetime.fromisoformat(
        dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update'])
    log.info(f'created: {event_date}')

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)

    collections = ()
    with conn.cursor() as cur:
        sql0 = "select collection_id from nft.events where event_date >= %s - Interval 1 day and price>0 group by collection_id order by sum(price) desc limit 50"
        cur.execute(sql0, event_date)
        result = cur.fetchall()
        conn.close()

    for i in result:
        collections.add(i[0][0])

    with conn.cursor() as cur:
        sql = "select collection_id, price from nft.events where collection_id in (%s) and event_date >= %s - Interval 1 day;"
        log.info(f'sql: {sql}')

        cur.execute(sql, (collections, event_date))
        result = cur.fetchall()

        conn.close()

    collection_dict = {}
    for i in result:
        if i[0] not in collection_dict:
            collection_dict[i[0]] = [i[1]]
        else:
            collection_dict[i[0]] = collection_dict[i[0]] + [i[1]]

    collection_medians = []
    collection_counts = []
    for i in collection_dict.keys():
        median = statistics.median(collection_dict[i])
        count = len(collection_dict[i])
        collection_medians.append(median)
        collection_counts.append(count)

    # Weighted Median
    x = np.array(collection_medians)
    weights = np.array(collection_counts)

    sudo_index = robustats.weighted_median(x, weights)

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    with conn.cursor() as cur:
        row_values = (event_date, sudo_index)
        cur.execute('INSERT INTO nft.sudo_index (`event_date`,`price`) VALUES (%s, %s)', row_values)

        conn.commit()

        log.info("rds updated")


