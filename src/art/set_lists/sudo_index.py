import boto3
from util import sudocoins_logger
import json
import statistics
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
import pymysql
import os
from datetime import datetime

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

rds_host = os.environ['db_host']
name = os.environ['db_user']
password = os.environ['db_pw']
db_name = os.environ['db_name']
port = 3306


def lambda_handler(event, context):
    collections = [3248, 1084]

    event_date = datetime.fromisoformat(
        dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update'])
    log.info(f'created: {event_date}')

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    points = []

    with conn.cursor() as cur:
        sql = '''select t.price from nft.events t inner join (select nft_id, max(event_date) as MaxDate from nft.events where price>0 and collection_id=%s and event_date >= %s - interval 14 day group by nft_id) tm on t.nft_id = tm.nft_id and t.event_date = tm.MaxDate where price>0;'''
        log.info(f'sql: {sql}')

        for i in collections:
            cur.execute(sql, (i, event_date))
            result = cur.fetchall()
            log.info(result)
            points.append(list(result))

        conn.close()

    counter = 0
    sum = 0

    for j in points:
        for k in j:
            counter += 1
            sum += k[0]

    sudo_index = int(sum / Decimal(counter))
    log.info(f'sudo index: {sudo_index}')

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    with conn.cursor() as cur:
        row_values = (event_date, sudo_index)
        cur.execute('INSERT INTO nft.sudo_index (`event_date`,`price`) VALUES (%s, %s)', row_values)

        conn.commit()

        log.info("rds updated")


