import boto3
from util import sudocoins_logger
import json
import statistics
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
import pymysql
import os

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

rds_host = os.environ['db_host']
name = os.environ['db_user']
password = os.environ['db_pw']
db_name = os.environ['db_name']
port = 3306


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')

    collections = [1,2,3]

    event_date = dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update']
    log.info(f'created: {event_date}')

    log.info('about to connect')
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    points = []
    try:
        with conn.cursor() as cur:
            sql = '''select price from nft.events where price>0 and collection_id=%s and event_date >= %s - interval 7 day;'''
            log.info(f'sql: {sql}')

            for i in collections:
                cur.execute(sql, (i, event_date))
                result = cur.fetchall()
                points.append[result]

        conn.close()

        counter = 0
        sum = 0

        for j in points:
            for k in j:
                counter +=1
                sum += k

        sudo_index = sum / counter

        with conn.cursor() as cur:
            cur.execute(
                'INSERT INTO `nft_events`.`open_sea_events` (`event_date`, `price`) VALUES (%s, %s)', (event_date,sudo_index))
            conn.commit()
            log.info("rds updated")

        conn.close()

    except Exception as e:
        log.info(e)