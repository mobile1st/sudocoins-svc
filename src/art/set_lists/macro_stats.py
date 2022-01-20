import boto3
from util import sudocoins_logger
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
    event_date = datetime.fromisoformat(
        dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update'])
    log.info(f'created: {event_date}')

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)

    with conn.cursor() as cur:
        sql0 = "select date(event_date), sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - Interval 29 day group by date(event_date);"
        cur.execute(sql0, event_date)
        result = cur.fetchall()
        conn.close()

    log.info(f'len result: {len(result)}')

    chart_data = []
    for i in result:
        point = {
            "x": str(i[0]),
            "sales": i[1] / (10 ** 18),
            "buyers": i[2],
            "trades": i[3]
        }
        chart_data.append(point)

    update_expression = "SET points=:pts"
    exp_att = {
        ':pts': chart_data
    }
    dynamodb.Table('config').update_item(
        Key={'configKey': "macro_stats"},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=exp_att,
        ReturnValues="UPDATED_NEW"
    )
    log.info('data added to collection table')

    return






