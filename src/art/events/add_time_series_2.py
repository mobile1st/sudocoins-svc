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

    collection_id = art['collection_id']
    art_object = art['art_object']
    eth_sale_price = art['last_sale_price']
    collection_code = art['collection_code']

    log.info('about to connect')
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    try:
        with conn.cursor() as cur:

            sql = '''select t.nft_id, t.price from nft.events t inner join (select nft_id, max(event_date) as MaxDate from nft.events where price>0 and collection_id=%s group by nft_id) tm on t.nft_id = tm.nft_id and t.event_date = tm.MaxDate where price>0;'''
            sql2 = '''select date(event_date), min(price) from nft.events where event_date >= now() - interval 7 day and price>0 and collection_id=%s group by date(event_date);'''
            sql3 = '''select date(event_date), min(price) from nft.events where event_date >= now() - interval 14 day and price>0 and collection_id=%s group by date(event_date);'''
            sql4 = '''select date(event_date), sum(price) from nft.events where event_date >= now() - interval 14 day and collection_id=%s group by date(event_date);'''
            sql5 = '''select date(event_date), avg(price) from nft.events where event_date >= now() - interval 14 day and price>0 and collection_id=%s group by date(event_date);'''
            sql6 = '''select date(event_date), count(*) from nft.events where event_date >= now() - interval 14 day and collection_id=%s group by date(event_date);'''
            sql7 = '''select event_date, price as c from nft.events where event_date >= now() - interval 7 day and collection_id=%s;'''

            log.info(f'sql: {sql}')
            log.info('about to execute')
            cur.execute(sql, collection_id)
            log.info('executed')
            result = cur.fetchall()
            log.info('fetched')
            more_charts = result
            log.info('RDS queries for Floor, Median, and Max executed')

            log.info(f'sql: {sql2}')
            cur.execute(sql2, collection_id)
            result2 = cur.fetchall()
            floor_chart = result2
            log.info('RDS query for Floor Charts executed')

            statements = []
            statements.append(sql3)
            statements.append(sql4)
            statements.append(sql5)
            statements.append(sql6)
            statements.append(sql7)
            charts = []
            try:
                for i in range(len(statements)):
                    log.info(i)
                    cur.execute(statements[i], collection_id)
                    result = cur.fetchall()
                    chart_data2 = []
                    for k in range(len(result)):
                        if i == 0:
                            # floor chart
                            point = {
                                "x": str(result[k][0]),
                                "y": result[k][1] / (10 ** 18)
                            }
                            chart_data2.append(point)
                        elif i == 1:
                            # sum chart
                            point = {
                                "x": str(result[k][0]),
                                "y": result[k][1] / (10 ** 18)
                            }
                            chart_data2.append(point)
                        elif i == 2:
                            # avg chart
                            point = {
                                "x": str(result[k][0]),
                                "y": result[k][1] / (10 ** 18)
                            }
                            chart_data2.append(point)
                        elif i == 3:
                            # trades chart
                            point = {
                                "x": str(result[k][0]),
                                "y": result[k][1]
                            }
                            chart_data2.append(point)

                        elif i == 4:
                            try:
                                # sales scatter
                                utcTime = datetime.strptime(str(result[k][0]), "%Y-%m-%d %H:%M:%S")
                                epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                                epochTime = int(epochTime)
                                point = {
                                    "x": epochTime,
                                    "y": result[k][1] / (10 ** 18)
                                }
                                chart_data2.append(point)
                            except Exception as e:
                                log.info(f'status: failure - {e}')

                    charts.append(chart_data2)
                log.info("more charts created")

            except Exception as e:
                log.info(f'status: failure - {e}')

        conn.close()

        values1 = {}
        for k in more_charts:
            values1[k[0]] = k[1]
        med = statistics.median(values1.values())
        mins = min(values1.values())
        maxs = max(values1.values())

        chart_data = []
        for i in floor_chart:
            point = {
                "x": str(i[0]),
                "y": i[1] / (10 ** 18)
            }
            chart_data.append(point)
        floor_points = {
            "floor": chart_data
        }

        log.info(f'floor_data: {floor_points}')
        log.info(f'more charts: {charts}')

        dynamodb = boto3.resource('dynamodb')
        '''
        update_expression1 = "SET floor = :fl, median = :me, maximum = :ma, chart_data =:chd, more_charts=:mc,"
        update_expression2 = " sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(" \
                             "sales_volume, :start2) + :inc2, collection_name = :cn, preview_url = :purl, " \
                             "collection_address = :ca, collection_date=:cd, sort_idx=:si, collection_data=:colldata, " \
                             "open_sea=:os, rds_collection_id=:rdscollid"
        update_expression = update_expression1 + update_expression2
        log.info('about to make expression attributes')
        exp_att1 = {
            ':fl': mins,
            ':me': med,
            ':ma': maxs,
            ':chd': floor_points,
            ':mc': charts,
            ':rdscollid': collection_id
        }
        ex_att2 = {
            ':start': 0,
            ':inc': 1,
            ':start2': 0,
            ':inc2': eth_sale_price,
            ':cn': art_object.get('asset', {}).get('collection', {}).get('name'),
            ':purl': art_object.get('asset', {}).get('collection', {}).get('image_url'),
            ':ca': art_object.get('asset', {}).get('asset_contract', {}).get('address', "unknown"),
            ':cd': art_object.get('collection_date', "0"),
            ":si": "true",
            ":os": art_object.get('asset', {}).get('collection', {}).get('slug', ""),
            ":colldata": {
                "name": art_object.get('asset', {}).get('collection', {}).get('name'),
                "image_url": art_object.get('asset', {}).get('collection', {}).get('image_url'),
                "description": art_object.get('asset', {}).get('collection', {}).get('description', ""),
                "discord": art_object.get('asset', {}).get('collection', {}).get('discord_url', ""),
                "twitter": art_object.get('asset', {}).get('collection', {}).get('twitter_username', ""),
                "instagram": art_object.get('asset', {}).get('collection', {}).get('instagram_username', ""),
                "website": art_object.get('asset', {}).get('collection', {}).get('external_url', "")
            }
        }

        ex_att2.update(exp_att1)
        log.info('expression attributes merged')
        dynamodb.Table('collections').update_item(
            Key={'collection_id': collection_code},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=ex_att2,
            ReturnValues="UPDATED_NEW"
        )
        log.info('data added to collection table')
        '''
    except Exception as e:
        log.info(f'status: failure - {e}')

    return


