import boto3
from util import sudocoins_logger
import pymysql
import os
from datetime import datetime
from decimal import Decimal

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
        daily = "select date(event_date), sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - Interval 29 day and event_id=1 group by date(event_date);"
        hourly = '''select year(event_date), month(event_date), day(event_date), hour(event_date), sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - interval 1 day and event_id=1 group by day(event_date), hour(event_date) order by day(event_date) asc, hour(event_date) asc;'''

        cur.execute(daily, event_date)
        result = cur.fetchall()

        cur.execute(hourly, event_date)
        result2 = cur.fetchall()
        conn.close()

    log.info(f'len result: {len(result)}')

    chart_data = []
    for i in result:
        point = {
            "x": str(i[0]),
            "volume": round(i[1] / (10 ** 18), 3),
            "buyers": i[2],
            "trades": i[3]
        }
        chart_data.append(point)

    chart_data2 = []
    for i in result2:
        point = {
            "x": str(i[0]) + "-" + str(i[1]) + "-" + str(i[2]) + " " + str(i[3]) + ":00:00",
            "volume": round(i[4] / (10 ** 18), 3),
            "buyers": i[5],
            "trades": i[6]
        }
        chart_data2.append(point)

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)

    with conn.cursor() as cur:
        sql0 = "select event_date, price, usd_price from nft.sudo_index where event_date >= %s - Interval 1 day and index_type = %s;"
        sql_hour = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 1 hour and event_id=1;'''
        sql_day = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 1 day and event_id=1;'''
        sql_week = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 7 day and event_id=1;'''

        sql_hour2 = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 2 hour and event_date <= now() - Interval 1 hour and event_id=1;'''
        sql_day2 = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 2 day and event_date <= now() - Interval 1 day and event_id=1;'''
        sql_week2 = '''select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= now() - Interval 14 day and event_date <= now() - Interval 7 day and event_id=1;'''

        statements = [sql_hour, sql_day, sql_week, sql_hour2, sql_day2, sql_week2]
        results = []

        cur.execute(sql0, (event_date, "all"))
        result = cur.fetchall()

        for i in statements:
            cur.execute(i)
            tmp = cur.fetchall()
            results.append(tmp)

        conn.close()

    log.info(f'len result: {len(result)}')

    index_data = []
    for i in result:
        point = {
            "x": str(i[0]),
            "eth_price": i[1] / (10 ** 18),
            "usd_price": i[2]
        }
        index_data.append(point)

    for i in range(len(results)):
        if i == 0:
            hour = {
                "volume": round(results[i][0][0] / (10 ** 18), 3),
                "trades": results[i][0][2],
                "buyers": results[i][0][1],
                "volume_delta": Decimal(str((results[i][0][0] - results[3][0][0]) / results[3][0][0] * 100)).quantize(
                    Decimal('1.00')),
                "trades_delta": Decimal(str((results[i][0][2] - results[3][0][2]) / results[3][0][2] * 100)).quantize(
                    Decimal('1.00')),
                "buyers_delta": Decimal(str((results[i][0][1] - results[3][0][1]) / results[3][0][1] * 100)).quantize(
                    Decimal('1.00'))
            }
        elif i == 1:
            day = {
                "volume": round(results[i][0][0] / (10 ** 18), 3),
                "trades": results[i][0][2],
                "buyers": results[i][0][1],
                "volume_delta": Decimal(str((results[i][0][0] - results[4][0][0]) / results[4][0][0] * 100)).quantize(
                    Decimal('1.00')),
                "trades_delta": Decimal(str((results[i][0][2] - results[4][0][2]) / results[4][0][2] * 100)).quantize(
                    Decimal('1.00')),
                "buyers_delta": Decimal(str((results[i][0][1] - results[4][0][1]) / results[4][0][1] * 100)).quantize(
                    Decimal('1.00'))
            }
        if i == 2:
            week = {
                "volume": round(results[i][0][0] / (10 ** 18), 3),
                "trades": results[i][0][2],
                "buyers": results[i][0][1],
                "volume_delta": Decimal(str((results[i][0][0] - results[5][0][0]) / results[5][0][0] * 100)).quantize(
                    Decimal('1.00')),
                "trades_delta": Decimal(str((results[i][0][2] - results[5][0][2]) / results[5][0][2] * 100)).quantize(
                    Decimal('1.00')),
                "buyers_delta": Decimal(str((results[i][0][1] - results[5][0][1]) / results[5][0][1] * 100)).quantize(
                    Decimal('1.00'))
            }

    update_expression = "SET daily_data=:pts, index_data=:idx, #d=:d, #w=:w, #h=:h, hourly_data=:hr"
    exp_att = {
        ':pts': chart_data,
        ':idx': index_data,
        ':h': hour,
        ':d': day,
        ':w': week,
        ':hr': chart_data2
    }

    dynamodb.Table('Config').update_item(
        Key={'configKey': "macro_stats"},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=exp_att,
        ExpressionAttributeNames={'#d': 'day', '#w': 'week', '#h': 'hour'},
        ReturnValues="UPDATED_NEW"
    )
    log.info('data added to collection table')

    return {

    }






