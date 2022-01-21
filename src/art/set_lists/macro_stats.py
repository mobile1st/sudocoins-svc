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
            "sales": round(i[1] / (10 ** 18), 3),
            "buyers": i[2],
            "trades": i[3]
        }
        chart_data.append(point)

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)

    with conn.cursor() as cur:
        sql0 = "select event_date, price, usd_price from nft.sudo_index where event_date >= %s - Interval 7 day and index_type = %s;"
        cur.execute(sql0, (event_date, "all"))
        result = cur.fetchall()
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

    with conn.cursor() as cur:
        sql_hour = "select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - Interval 1 day ;"
        sql_day = "select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - Interval 7 day;"
        sql_week = "select sum(price), count(distinct buyer_id), count(*) from nft.events where event_date >= %s - Interval 30 day ;"

        statements = [sql_hour, sql_day, sql_week]
        results = []
        for i in statements:
            cur.execute(sql0, event_date)
            tmp = cur.fetchall()
            results.append(tmp)
        conn.close()


    for i in range(len(results)):
        if i == 0:
            day = {
                "volume": results[i][0][0],
                "trades": results[i][0][2],
                "buyers": results[i][0][1]
            }
        elif i == 1:
            week = {
                "volume": results[i][0][0],
                "trades": results[i][0][2],
                "buyers": results[i][0][1]
            }
        if i == 2:
            month = {
                "volume": results[i][0][0],
                "trades": results[i][0][2],
                "buyers": results[i][0][1]
            }


    update_expression = "SET points=:pts, index_points=:idx, day=:d, week=:w, month=:m"
    exp_att = {
        ':pts': chart_data,
        ':idx': index_data,
        ':d': day,
        ':w': week,
        ':m': month
    }

    dynamodb.Table('Config').update_item(
        Key={'configKey': "macro_stats"},
        UpdateExpression=update_expression,
        ExpressionAttributeValues=exp_att,
        ReturnValues="UPDATED_NEW"
    )
    log.info('data added to collection table')

    return






