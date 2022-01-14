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
    collection_url = art['collection_url']

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
            sql7 = '''select event_date, price as c from nft.events where event_date >= now() - interval 1 day and collection_id=%s;'''

            log.info(f'sql: {sql}')
            cur.execute(sql, collection_id)
            more_charts = cur.fetchall()
            log.info('RDS queries for Floor, Median, and Max executed')

            log.info(f'sql: {sql2}')
            cur.execute(sql2, collection_id)
            result2 = cur.fetchall()
            floor_chart = result2
            log.info('RDS query for Floor Charts executed')

            statements = [sql3, sql4, sql5, sql6]
            statements.append(sql7)

            results = []

            for i in range(len(statements)):
                cur.execute(statements[i], collection_id)
                result = cur.fetchall()
                results.append(result)

            conn.close()

        try:
            charts = []
            for i in range(len(results)):
                chart_points = []
                for k in results[i]:
                    if i == 0:
                        # floor chart
                        point = {
                            "x": str(k[0]),
                            "y": k[1] / (10 ** 18)
                        }
                        chart_points.append(point)
                    elif i == 1:
                        # sum chart
                        point = {
                            "x": str(k[0]),
                            "y": k[1] / (10 ** 18)
                        }
                        chart_points.append(point)
                    elif i == 2:
                        # avg chart
                        point = {
                            "x": str(k[0]),
                            "y": k[1] / (10 ** 18)
                        }
                        chart_points.append(point)
                    elif i == 3:
                        # trades chart
                        point = {
                            "x": str(k[0]),
                            "y": k[1]
                        }
                        chart_points.append(point)

                    elif i == 4:
                        try:
                            # sales scatter
                            utcTime = datetime.strptime(str(k[0]), "%Y-%m-%d %H:%M:%S")

                            epochTime = int((utcTime - datetime(1970, 1, 1)).total_seconds())
                            epochTime = int(epochTime)
                            point = {
                                "x": epochTime,
                                "y": k[1] / (10 ** 18)
                            }
                            chart_points.append(point)
                        except Exception as e:
                            log.info(f'status: failure - {e}')
                            log.info(str(k[0]))

                charts.append(chart_points)
            log.info("more charts created")

        except Exception as e:
            log.info(f'status: failure - {e}')

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


    except Exception as e:
        log.info(f'status: failure - {e}')

    try:
        daily_data = get_day(collection_id)
        log.info('success daily')
        log.info(daily_data)
    except Exception as e:
        log.info(f'issue: {e}')

    try:
        weekly_data, monthly_data = get_week_month(collection_id)
        log.info('success weekly and monthly')
    except Exception as e:
        log.info(f'issue: {e}')

    last_update = "false"

    try:
        update_expression1 = "SET floor = :fl, median = :me, maximum = :ma, chart_data =:chd, more_charts=:mc,"
        update_expression2 = " sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(" \
                             "sales_volume, :start2) + :inc2, collection_name = :cn, preview_url = :purl, " \
                             "collection_address = :ca, collection_date=:cd, sort_idx=:si, collection_data=:colldata, last_update=:lasup," \
                             "open_sea=:os, rds_collection_id=:rdscollid, blockchain=:bc, collection_url=:curl, daily_data=:daily, weekly_data=:weekly, monthly_data=:monthly"
        update_expression = update_expression1 + update_expression2
        log.info('about to make expression attributes')
        exp_att1 = {
            ':fl': mins,
            ':curl': collection_url,
            ':me': med,
            ':ma': maxs,
            ':chd': floor_points,
            ':lasup': last_update,
            ':mc': charts,
            ':rdscollid': collection_id,
            ':bc': art_object.get('blockchain'),
            ':daily': daily_data,
            ':weekly': weekly_data,
            ':monthly': monthly_data
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
    except Exception as e:
        log.info(f'status: failure - {e}')

    return


def get_day(collection_id):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    try:
        with conn.cursor() as cur:

            floor = '''select date(event_date), min(price) from nft.events where event_date >= curdate() and price>0 and collection_id=%s group by date(event_date);'''
            ceiling = '''select date(event_date), max(price) from nft.events where event_date >= curdate() and price>0 and collection_id=%s group by date(event_date);'''
            volume = '''select date(event_date), sum(price) from nft.events where event_date >= curdate() and collection_id=%s group by date(event_date);'''
            trades = '''select date(event_date), count(*) from nft.events where event_date >= curdate() and collection_id=%s and price>0  group by date(event_date);'''
            buyers = '''select date(event_date), count(distinct buyer_id) from nft.events where event_date >= curdate() and collection_id=%s group by date(event_date);'''
            avg_per_hour = '''select count(*) / HOUR(now()) from nft.events where event_date >= curdate() and collection_id=%s and price>0;'''

            floor_points = '''select year(event_date), month(event_date), day(event_date), hour(event_date), min(price) from nft.events where event_date >= now() - interval 1 day and collection_id=%s group by day(event_date), hour(event_date) order by day(event_date) asc, hour(event_date) asc;'''
            ceiling_points = '''select year(event_date), month(event_date), day(event_date), hour(event_date), max(price) from nft.events where event_date >= now() - interval 1 day and collection_id=%s group by day(event_date), hour(event_date) order by day(event_date) asc, hour(event_date) asc;'''
            volume_points = '''select year(event_date), month(event_date), day(event_date), hour(event_date), sum(price) from nft.events where event_date >= now() - interval 1 day and collection_id=%s group by day(event_date), hour(event_date) order by day(event_date) asc, hour(event_date) asc;'''
            trades_points = '''select year(event_date), month(event_date), day(event_date), hour(event_date), count(*) from nft.events where event_date >= now() - interval 1 day and collection_id=%s group by day(event_date), hour(event_date);'''
            buyers_points = '''select year(event_date), month(event_date), day(event_date), hour(event_date), count(distinct buyer_id) from nft.events where event_date >= now() - interval 1 day and collection_id=%s group by day(event_date), hour(event_date);'''

            results = []
            statements = [floor, ceiling, volume, trades, buyers, avg_per_hour, floor_points, ceiling_points,
                          volume_points, trades_points, buyers_points]
            for i in statements:
                cur.execute(i, collection_id)
                tmp = cur.fetchall()
                results.append(tmp)

            conn.close()
        log.info("0")
        log.info(results[0])
        log.info(results[1])
        log.info(results[2])
        log.info(results[3])
        log.info(results[4])
        log.info(results[5])

        daily_data = {
            "floor": results[0][0][1] / (10 ** 18),
            "ceiling": results[1][0][1] / (10 ** 18),
            "volume": results[2][0][1] / (10 ** 18),
            "trades": results[3][0][1],
            "buyers": results[4][0][1],
            "avg_per_hour": results[5][0][0]
        }
        log.info("1")
        try:
            charts = []
            for i in range(6, 11):
                chart_points = []
                for k in results[i]:
                    if i in [6, 7, 8]:
                        point = {
                            "x": str(k[0]) + "-" + str(k[1]) + "-" + str(k[2]) + " " + str(k[3]) + ":00:00",
                            "y": k[4] / (10 ** 18)
                        }
                        chart_points.append(point)
                    else:
                        point = {
                            "x": str(k[0]) + "-" + str(k[1]) + "-" + str(k[2]) + " " + str(k[3]) + ":00:00",
                            "y": k[4]
                        }
                        chart_points.append(point)
                charts.append(chart_points)

            daily_data['floor_points'] = charts[0]
            daily_data['ceiling_points'] = charts[1]
            daily_data['volume_points'] = charts[2]
            daily_data['trades_points'] = charts[3]
            daily_data['buyers_points'] = charts[4]

        except Exception as e:
            log.info(f'status: failure - {e}')
            log.info(f'collection_id - {collection_id}')

        return daily_data

    except Exception as e:
        log.info(f'status: failure - {e}')
        log.info(f'collection_id - {collection_id}')


def get_week_month(collection_id):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    try:
        with conn.cursor() as cur:

            floor = '''select min(price) from nft.events where event_date >= curdate() - interval 6 day and price>0 and collection_id=%s ;'''
            ceiling = '''select max(price) from nft.events where event_date >= curdate() - interval 6 day and price>0 and collection_id=%s ;'''
            volume = '''select sum(price) from nft.events where event_date >= curdate() - interval 6 day and collection_id=%s ;'''
            trades = '''select count(*) from nft.events where event_date >= curdate() - interval 6 day and collection_id=%s and price>0  ;'''
            buyers = '''select count(distinct buyer_id) from nft.events where event_date >= curdate() - interval 6 day and collection_id=%s ;'''
            avg_per_day = '''select count(*) / (6 + (HOUR(now())/24)) from nft.events where event_date >= curdate() - interval 6 day and collection_id=%s and price>0;'''

            floor2 = '''select min(price) from nft.events where event_date >= curdate() - interval 30 day and price>0 and collection_id=%s ;'''
            ceiling2 = '''select max(price) from nft.events where event_date >= curdate() - interval 30 day and price>0 and collection_id=%s ;'''
            volume2 = '''select sum(price) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s ;'''
            trades2 = '''select count(*) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price>0 ;'''
            buyers2 = '''select count(distinct buyer_id) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s ;'''
            avg_per_day2 = '''select count(*) / (30 + (HOUR(now())/24)) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price>0;'''

            floor_points = '''select date(event_date), min(price) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price >0 group by date(event_date);'''
            ceiling_points = '''select date(event_date), max(price) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price >0 group by date(event_date);'''
            volume_points = '''select date(event_date), sum(price) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price >0 group by date(event_date);'''
            trades_points = '''select date(event_date), count(*) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price >0 group by date(event_date);'''
            buyers_points = '''select date(event_date),  count(distinct buyer_id) from nft.events where event_date >= curdate() - interval 30 day and collection_id=%s and price >0 group by date(event_date);'''

            results = []
            statements = [floor, ceiling, volume, trades, buyers, avg_per_day, floor2, ceiling2, volume2, trades2,
                          buyers2, avg_per_day2, floor_points, ceiling_points,
                          volume_points, trades_points, buyers_points]
            for i in statements:
                cur.execute(i, collection_id)
                tmp = cur.fetchall()
                results.append(tmp)

            conn.close()

        weekly_data = {
            "floor": results[0][0][0] / (10 ** 18),
            "ceiling": results[1][0][0] / (10 ** 18),
            "volume": results[2][0][0] / (10 ** 18),
            "trades": results[3][0][0],
            "buyers": results[4][0][0],
            "avg_per_day": results[5][0][0]
        }
        monthly_data = {
            "floor": results[6][0][0] / (10 ** 18),
            "ceiling": results[7][0][0] / (10 ** 18),
            "volume": results[8][0][0] / (10 ** 18),
            "trades": results[9][0][0],
            "buyers": results[10][0][0],
            "avg_per_day": results[11][0][0]
        }

        try:
            charts = []
            for i in range(12, 17):
                chart_points = []
                for k in results[i]:
                    if i in [12, 13, 14]:
                        point = {
                            "x": str(k[0]),
                            "y": k[1] / (10 ** 18)
                        }
                        chart_points.append(point)
                    else:
                        point = {
                            "x": str(k[0]),
                            "y": k[1]
                        }
                        chart_points.append(point)
                charts.append(chart_points)

            monthly_data['floor_points'] = charts[0]
            monthly_data['ceiling_points'] = charts[1]
            monthly_data['volume_points'] = charts[2]
            monthly_data['trades_points'] = charts[3]
            monthly_data['buyers_points'] = charts[4]

        except Exception as e:
            log.info(f'status: failure - {e}')

        log.info(f'weekly - {weekly_data}')
        log.info(f'monthly failure - {monthly_data}')
        return weekly_data, monthly_data

    except Exception as e:
        log.info(f'status: failure - {e}')



