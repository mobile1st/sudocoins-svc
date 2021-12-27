import boto3
from util import sudocoins_logger
import json
import statistics
from datetime import datetime, timedelta
from decimal import Decimal, getcontext
import pymysql

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client("sns")

rds_host = "rds-proxy.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
name = "admin"
password = "RHV2CiqtjiZpsM11"
db_name = "nft_events"
port = 3306


def lambda_handler(event, context):
    art = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'art: {art}')

    collection_id = art['collection_id']
    log.info('about to connect')
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    try:
        with conn.cursor() as cur:
            if collection_id.find("'") != -1:
                sql = '''select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and collection_id="''' + collection_id + '''" group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;'''
                sql2 = '''select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 7 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql3 = '''select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql4 = '''select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql5 = '''select date(event_date), avg(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql6 = '''select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql7 = '''select event_date, price as c from nft_events.open_sea_events where event_date >= now() - interval 17 day and collection_id ="''' + collection_id + '''" group by date(event_date);'''

            elif collection_id.find('"') != -1:
                sql = """select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and collection_id='""" + collection_id + """' group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;"""
                sql2 = """select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 7 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql3 = """select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql4 = """select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql5 = """select date(event_date), avg(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql6 = """select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql7 = """select event_date, price as c from nft_events.open_sea_events >= now() - interval 7 day and collection_id ='""" + collection_id + """' group by date(event_date);'"""

            else:
                sql = '''select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and  collection_id="''' + collection_id + '''" group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;'''
                sql2 = '''select date(event_date), min(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 7 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql3 = '''select date(event_date), min(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql4 = '''select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql5 = '''select date(event_date), avg(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql6 = '''select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql7 = '''select event_date, price as c from nft_events.open_sea_events where event_date >= now() - interval 7 day and collection_id ="''' + collection_id + '''" group by date(event_date);'''

            log.info(f'sql: {sql}')
            cur.execute(sql)
            result = cur.fetchall()
            more_charts = result
            log.info('RDS queries for Floor, Median, and Max executed')

            log.info(f'sql: {sql2}')
            cur.execute(sql2)
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
                    cur.execute(statements[i])
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
                                #sales scatter
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
                log.info(e)

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

        dynamodb.Table('collections').update_item(
            Key={'collection_id': collection_id},
            UpdateExpression="SET floor = :fl, median = :me, maximum = :ma, chart_data =:cd, more_charts=:mc",
            ExpressionAttributeValues={
                ':fl': mins,
                ':me': med,
                ':ma': maxs,
                ':cd': floor_points,
                ':mc': charts
            },
            ReturnValues="UPDATED_NEW"
        )
        log.info('data added to collection table')


    except Exception as e:
        log.info(f'status: failure - {e}')

    return


