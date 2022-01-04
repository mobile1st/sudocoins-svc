import boto3
from util import sudocoins_logger
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta
import os
import pymysql

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')

rds_host = os.environ['db_host']
name = os.environ['db_user']
password = os.environ['db_pw']
db_name = os.environ['db_name']
port = 3306


def lambda_handler(event, context):
    nfts_hour, nfts_day, nfts_week, buyers_hour, buyers_day, buyers_week = get_trending()

    set_config(nfts_hour, nfts_day, nfts_week, buyers_hour, buyers_day, buyers_week)

    return


def set_config(nfts_hour, nfts_day, nfts_week, buyers_hour, buyers_day, buyers_week):
    config_table = dynamodb.Table('Config')

    if len(nfts_hour) == 0:
        trending_nfts = nfts_day
    else:
        trending_nfts = nfts_hour

    config_table.update_item(
        Key={
            'configKey': 'TrendingNFTs'
        },
        UpdateExpression="set art=:art, trending_hour=:hour, trending_half_day=:hday, trending_day=:day",
        ExpressionAttributeValues={
            ":art": trending_nfts,
            ":hour": nfts_hour,
            ":hday": nfts_day,
            ":day": nfts_week
        },
        ReturnValues="ALL_NEW"
    )

    config_table.update_item(
        Key={'configKey': 'TopBuyers2'},
        UpdateExpression="set buyers_day=:buy, buyers_hour=:buy2, buyers_half=:buy3",
        ExpressionAttributeValues={
            ":buy": buyers_day,
            ":buy2": buyers_hour,
            ":buy3": buyers_week
        }
    )

    log.info("configs updated")


def get_trending():
    time_now = str(datetime.utcnow().isoformat())
    log.info(f'time_now: {time_now}')
    start_time = dynamodb.Table('Config').get_item(Key={'configKey': 'ingest2'})['Item']['last_update']

    hour = (datetime.fromisoformat(start_time) - timedelta(hours=1)).isoformat()
    day = (datetime.fromisoformat(start_time) - timedelta(days=1)).isoformat()
    week = (datetime.fromisoformat(start_time) - timedelta(days=7)).isoformat()

    log.info('about to connect')
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    try:
        with conn.cursor() as cur:

            sql = '''select nf.art_code, max(price) as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 7 day group by ev.nft_id order by a desc limit 250;'''
            sql2 = '''select nf.art_code, max(price) as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 1 day group by ev.nft_id order by a desc limit 250;'''
            sql3 = '''select nf.art_code, max(price) as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 1 hour group by ev.nft_id order by a desc limit 250;'''

            statements = [sql,sql2,sql3]
            times = [week, day, hour]

            nfts = []
            try:
                for i in range(len(statements)):
                    cur.execute(statements[i], times[i])
                    result = cur.fetchall()
                    nfts.append(result)
            except Exception as e:
                log.info(f'status: failure - {e}')
            '''
            sql = "select public_key, sum(price), avatar as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 7 day group by ev.nft_id order by a desc limit 250;"
            sql2 = "select public_key, sum(price), avatar as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 1 day group by ev.nft_id order by a desc limit 250;"
            sql3 = "select public_key, sum(price), avatar as a from nft.events ev inner join nft.nfts nf on ev.nft_id = nf.id where ev.event_date >= %s - interval 1 hour group by ev.nft_id order by a desc limit 250;"

            statements = [sql, sql2, sql3]
            times = [week, day, hour]

            nfts = []
            try:
                for i in range(len(statements)):
                    cur.execute(statements[i], times[i])
                    result = cur.fetchall()
                    nfts.append(result)
            except Exception as e:
                log.info(f'status: failure - {e}')
            
            '''

    except Exception as e:
        log.info(f'status: failure - {e}')

    nfts_week = []
    nfts_day = []
    nfts_hour = []
    nfts_list = [nfts_week, nfts_day, nfts_hour]
    for i in range(len(nfts)):
        for k in nfts[i]:
            nfts_list[0].append(k[0])

    return nfts_list[0], nfts_list[1], nfts_list[2], [], [], []
