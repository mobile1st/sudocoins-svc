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
rds_host2 = os.environ['db_host2']
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
    twitter = art['twitter']

    last_update = str(datetime.utcnow().isoformat())

    try:
        collection_record = dynamodb.Table('collections').get_item(
            Key={'collection_id': collection_code}
        )

        if 'Item' in collection_record:
            updated = collection_record['Item'].get('last_update')

            if updated is None or updated == 'true' or updated == 'false':
                log.info('no last_update')
                log.info(updated)
                difference = 60
            else:
                log.info(last_update)
                log.info(updated)
                difference = (datetime.fromisoformat(last_update) - datetime.fromisoformat(
                    str(updated))).total_seconds() / 60

        if 'Item' in collection_record and difference >= 60:

            med, mins, maxs, floor_points, charts = get_charts(collection_id)
            trades = get_trades_delta(collection_id)
            try:
                floor_snapshot(collection_id)
                log.info("floor snapshot complete")
            except Exception as e:
                log.info(e)
            try:
                if 'percentage_total_owners' in collection_record['Item']:
                    log.info(collection_record['Item']['percentage_total_owners'])
                    owner_asset_snapshot(collection_id, collection_record['Item']['percentage_total_owners'])
                    log.info("oa snapshot complete")
            except Exception as e:
                log.info(e)
            try:
                score = generate_score(collection_id, collection_record)
                log.info("score calculated")
            except Exception as e:
                log.info(e)

            update_expression1 = "SET floor = :fl, median = :me, maximum = :ma, chart_data =:chd, more_charts=:mc,"
            update_expression2 = " sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(" \
                                 "sales_volume, :start2) + :inc2, score=:scor,  " \
                                 "last_update=:lasup, os_update=:osup," \
                                 "collection_url=:curl, sales_delta=:td"
            update_expression = update_expression1 + update_expression2
            exp_att1 = {
                ':fl': mins,
                ':curl': art_object.get('asset', {}).get('collection', {}).get('slug', ""),
                ':me': med,
                ':ma': maxs,
                ':chd': floor_points,
                ':lasup': last_update,
                ':mc': charts,
                ':osup': 'false',
                ':td': trades,
                ':scor': score
            }
            ex_att2 = {
                ':start': 0,
                ':inc': 1,
                ':start2': 0,
                ':inc2': eth_sale_price
            }
            ex_att2.update(exp_att1)
            dynamodb.Table('collections').update_item(
                Key={'collection_id': collection_code},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=ex_att2,
                ReturnValues="UPDATED_NEW"
            )
            log.info('data added to collection table')

            try:
                msg = {
                    "collection_id": collection_id,
                    'twitter': twitter,
                    'collection_code': collection_code,
                    'collection_url': art_object.get('asset', {}).get('collection', {}).get('slug', "")
                }

                log.info(msg)

                sns_client.publish(
                    TopicArn='arn:aws:sns:us-west-2:977566059069:AddScoreTopic',
                    MessageStructure='string',
                    Message=json.dumps(msg)
                )

                log.info(f"add time series published")
            except Exception as e:
                log.info(f"status: failure - {e}")

        elif 'Item' not in collection_record:

            med, mins, maxs, floor_points, charts = get_charts(collection_id)
            trades = get_trades_delta(collection_id)

            update_expression1 = "SET floor = :fl, median = :me, maximum = :ma, chart_data =:chd, more_charts=:mc,"
            update_expression2 = " sale_count = if_not_exists(sale_count, :start) + :inc, sales_volume = if_not_exists(" \
                                 "sales_volume, :start2) + :inc2, collection_name = :cn, preview_url = :purl, " \
                                 "collection_address = :ca, collection_date=:cd, sort_idx=:si, collection_data=:colldata, last_update=:lasup," \
                                 "open_sea=:os, rds_collection_id=:rdscollid, blockchain=:bc, collection_url=:curl, os_update=:osup," \
                                 "sales_delta=:td"
            update_expression = update_expression1 + update_expression2
            exp_att1 = {
                ':fl': mins,
                ':curl': art_object.get('asset', {}).get('collection', {}).get('slug', ""),
                ':me': med,
                ':ma': maxs,
                ':chd': floor_points,
                ':lasup': last_update,
                ':mc': charts,
                ':rdscollid': collection_id,
                ':bc': art_object.get('blockchain'),
                ':osup': 'false',
                ':td': trades
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


def get_charts(collection_id):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    try:
        with conn.cursor() as cur:

            sql = '''select t.nft_id, t.price from nft.events t inner join (select nft_id, max(event_date) as MaxDate from nft.events where event_id = 1 and collection_id=%s and price>0 group by nft_id) tm on t.nft_id = tm.nft_id and t.event_date = tm.MaxDate where event_id=1 and price>0;'''
            sql2 = '''select date(event_date), min(price) from nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 7 day group by date(event_date);'''
            sql7 = '''select event_date, price as c from nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 1 day;'''

            cur.execute(sql, collection_id)
            more_charts = cur.fetchall()

            cur.execute(sql2, collection_id)
            result2 = cur.fetchall()
            floor_chart = result2

            statements = []
            statements.append(sql7)
            results = []

            for i in range(len(statements)):
                cur.execute(statements[i], collection_id)
                result = cur.fetchall()
                results.append(result)

            conn.close()

        try:
            charts = {}
            for i in range(len(results)):
                chart_points = []
                for k in results[i]:
                    if i == 0:
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

                charts['scatter'] = chart_points

        except Exception as e:
            log.info(f'status: failure - {e}')

        values1 = {}
        for k in more_charts:
            values1[k[0]] = k[1]
        if len(values1) == 0:
            med = 0
            mins = 0
            maxs = 0
        else:
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

        return med, mins, maxs, floor_points, charts

    except Exception as e:
        log.info(f'status: failure - {e}')


def get_trades_delta(collection_id):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    # period = "day"

    with conn.cursor() as cur:
        sql = "SELECT distinct co.collection_code, t2.count2, t1.count1, round(((t1.count1-t2.count2)/t2.count2*100),1) AS delta FROM (SELECT collection_id, COUNT(*) AS count1 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 1 day GROUP BY collection_id) t1 LEFT JOIN (SELECT collection_id, COUNT(*) AS count2 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 2 day and event_date <= now() - interval 1 day GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id INNER JOIN nft.collections co on co.id=t1.collection_id ;"
        log.info(sql)
        cur.execute(sql, (collection_id, collection_id))
        result = cur.fetchall()

    trades = {
        "yesterday": result[0][1],
        "today": result[0][2],
        "delta": result[0][3]
    }

    return trades


def floor_snapshot(collection_id):
    conn = pymysql.connect(host=rds_host2, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    # period = "day"

    with conn.cursor() as cur:
        sql = "select t.nft_id, t.price from nft.events t inner join (select nft_id, max(event_date) as MaxDate from nft.events where collection_id=1084 and event_date > '2022-02-08T22:37:06.111111' group by nft_id) tm on t.nft_id = tm.nft_id and t.event_date = tm.MaxDate where event_id in (3,4) and price>0 and price is not null order by t.price asc limit 1;"
        log.info(sql)
        cur.execute(sql)
        result = cur.fetchall()
        floor = result[0][1]
        nft_id = result[0][0]
        time_now = str(datetime.utcnow().isoformat())

        row_values = (collection_id, nft_id, floor, time_now)
        cur.execute(
            'INSERT INTO `nft`.`floor_charts` (`collection_id`,`nft_id`,`price`, `created_date`) VALUES (%s, %s, %s, %s)',
            row_values)
        conn.commit()
        conn.close()
    log.info('snapshot created')


def owner_asset_snapshot(collection_id, oa_ratio):
    conn = pymysql.connect(host=rds_host2, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')

    with conn.cursor() as cur:
        time_now = str(datetime.utcnow().isoformat())

        row_values = (collection_id, oa_ratio, time_now)
        cur.execute(
            'INSERT INTO `nft`.`owner_asset_ratios` (`collection_id`,`ratio`, `created_date`) VALUES (%s, %s, %s)',
            row_values)
        conn.commit()
        conn.close()
    log.info('snapshot created')


def generate_score(collection_id, collection_record):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    with conn.cursor() as cur:
        sql = "SELECT round(((t1.count1-t2.count2)/t2.count2*100),1) AS delta FROM (SELECT collection_id, COUNT(*) AS count1 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 7 day GROUP BY collection_id) t1 LEFT JOIN (SELECT collection_id, COUNT(*) AS count2 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 14 day and event_date <= now() - interval 7 day GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id INNER JOIN nft.collections co on co.id=t1.collection_id;"
        sql2 = "SELECT round(((t1.count1-t2.count2)/t2.count2*100),1) AS delta FROM (SELECT collection_id, sum(price) AS count1 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 7 day GROUP BY collection_id) t1 LEFT JOIN (SELECT collection_id, sum(price) AS count2 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 14 day and event_date <= now() - interval 7 day GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id INNER JOIN nft.collections co on co.id=t1.collection_id;"
        sql3 = "SELECT round(((t1.count1-t2.count2)/t2.count2*100),1) AS delta FROM (SELECT collection_id, avg(price) AS count1 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 7 day GROUP BY collection_id) t1 LEFT JOIN (SELECT collection_id, avg(price) AS count2 FROM nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 14 day and event_date <= now() - interval 7 day GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id INNER JOIN nft.collections co on co.id=t1.collection_id;"

        statements = [sql, sql2, sql3]
        results = []
        for i in range(len(statements)):
            cur.execute(statements[i], (collection_id, collection_id))
            result = cur.fetchall()
            results.append(result)

        conn.close()
    score = 0
    for i in results:
        log.info(i[0][0])
        if i[0][0] > 0:
            score += 15
    if collection_record['Item']['percentage_total_owners'] > 50:
        score += 15

    if 'followers' in collection_record['Item']:
        if collection_record['Item']['followers'] > 5000 and collection_record['Item']['followers'] < 10000:
            score += 10
        elif collection_record['Item']['followers'] > 10000 and collection_record['Item']['followers'] < 25000:
            score += 15
        elif collection_record['Item']['followers'] > 25000:
            score += 20

    if 'ipfs' in collection_record['Item']:
        if 'ipfs' == 'true':
            score += 20

    log.info(score)

    return


def charts_30(collection_id):
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    log.info('connection established')
    with conn.cursor() as cur:
        sql = "select date(event_date), sum(price) from nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 30 day group by event_date;"
        sql2 = "select date(event_date), sum(price) from nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 30 day group by event_date;"
        sql3 = "select date(event_date), sum(price) from nft.events where event_id=1 and collection_id=%s and event_date >= now() - interval 30 day group by event_date;"

        statements = [sql, sql2, sql3]
        results = []
        for i in range(len(statements)):
            cur.execute(statements[i], (collection_id, collection_id))
            result = cur.fetchall()
            results.append(result)

        conn.close()

    return
