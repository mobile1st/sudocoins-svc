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

    timestamp = art['event_date'].split('T')[0]
    lsp = art['last_sale_price']
    art_id = art['art_id']

    collection_id = art['collection_id']
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=5)
    try:
        with conn.cursor() as cur:
            if collection_id.find("'") != -1:
                sql = '''select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and collection_id="''' + collection_id + '''" group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;'''
                sql2 = '''select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 7 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql3 = '''select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql4 = '''select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql5 = '''select date(event_date), avg(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql6 = '''select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
            elif collection_id.find('"') != -1:
                sql = """select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and collection_id='""" + collection_id + """' group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;"""
                sql2 = """select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 7 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql3 = """select date(event_date), min(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql4 = """select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql5 = """select date(event_date), avg(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and price>0 and collection_id='""" + collection_id + """' group by date(event_date);"""
                sql6 = """select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id='""" + collection_id + """' group by date(event_date);"""

            else:
                sql = '''select t.art_id, t.price from nft_events.open_sea_events t inner join (select art_id, max(event_date) as MaxDate from nft_events.open_sea_events where price>0 and  collection_id="''' + collection_id + '''" group by art_id) tm on t.art_id = tm.art_id and t.event_date = tm.MaxDate where price>0;'''
                sql2 = '''select date(event_date), min(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 7 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql3 = '''select date(event_date), min(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql4 = '''select date(event_date), sum(price) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql5 = '''select date(event_date), avg(price) from nft_events.open_sea_events where price>0 and created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''
                sql6 = '''select date(event_date), count(*) from nft_events.open_sea_events where created_date >= now() - interval 14 day and collection_id="''' + collection_id + '''" group by date(event_date);'''

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
            charts = []
            try:
                for i in range(len(statements)):
                    cur.execute(statements[i])
                    result = cur.fetchall()
                    log.info('more charts executed')
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

                    charts.append(chart_data2)
                log.info(charts)

            except Exception as e:
                log.info(e)

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

        dynamodb.Table('collections').update_item(
            Key={'collection_id': collection_id},
            UpdateExpression="SET floor = :fl, median = :me, maximum = :ma, chart_data =:cd, more_charts=:mc",
            ExpressionAttributeValues={
                ':fl': mins,
                ':me': med,
                ':ma': maxs,
                ':cd': chart_data,
                ':mc': charts
            },
            ReturnValues="UPDATED_NEW"
        )
        log.info('floor median and max added to collection table')


    except Exception as e:
        log.info(e)

    update_collection(collection_id)

    return


def update_collection(collection_id):
    time_series = str(datetime.utcnow().isoformat()).split('T')[0]
    time_list = []
    count = 6
    while count > 0:
        new_time = (datetime.utcnow() - timedelta(days=count)).isoformat().split('T')[0]
        time_list.append(new_time)
        count -= 1
    time_list.append(time_series)

    final_series = {}
    keys_list = []

    for k in time_list:
        tmp = {
            "date": k,
            "collection_id": collection_id
        }
        keys_list.append(tmp)

    query = {
        'Keys': keys_list,
        'ProjectionExpression': '#d, trades',
        'ExpressionAttributeNames': {'#d': 'date'}
    }
    response = dynamodb.batch_get_item(RequestItems={'time_series': query})

    final_series['floor'] = []
    getcontext().prec = 18

    for row in response['Responses']['time_series']:
        final_series['floor'].insert(0, {"x": row['date'],
                                         "y": Decimal(min(row['trades'])) / (10 ** 18)})

    floor_list = final_series['floor']
    new_floor_list = sorted(floor_list, key=lambda i: i['x'], reverse=False)
    final_series['floor'] = new_floor_list

    dynamodb.Table('collections').update_item(
        Key={
            'collection_id': collection_id
        },
        UpdateExpression="SET chart_data = :cd",
        ExpressionAttributeValues={
            ':cd': final_series
        },
        ReturnValues="UPDATED_NEW"
    )

    log.info(f'collection table updated: {collection_id}')

    return

