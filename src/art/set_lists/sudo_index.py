import boto3
from util import sudocoins_logger
import statistics
import pymysql
import os
from datetime import datetime
import numpy as np
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

    configTable = dynamodb.Table('Config')
    configKey = "HomePage"

    response = configTable.get_item(Key={'configKey': configKey})
    config = response['Item']

    eth_rate = str(config['ethRate'])

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)

    with conn.cursor() as cur:
        sql0 = "select collection_id, price from nft.events where collection_id in (select collection_id from nft.events where event_date >= now() - Interval 1 day and price>0 group by collection_id) and event_date >= %s - interval 1 day and event_id=1;"
        cur.execute(sql0, event_date)
        result = cur.fetchall()
        conn.close()

    log.info(f'len result: {len(result)}')

    collection_sum = {}
    for i in result:
        if i[0] not in collection_sum:
            collection_sum[i[0]] = {'sales': [], 'sum': 0}
            collection_sum[i[0]]['sales'] = [i[1]]
            collection_sum[i[0]]['sum'] = i[1]
        else:
            collection_sum[i[0]]['sales'] = collection_sum[i[0]]['sales'] + [i[1]]
            collection_sum[i[0]]['sum'] = collection_sum[i[0]]['sum'] + i[1]

    top_50 = sorted(collection_sum.keys(), key=lambda x: collection_sum[x]['sum'])[-50:]

    collection_medians = []
    collection_counts = []

    for k in top_50:
        median = statistics.median(collection_sum[k]['sales'])
        count = len(collection_sum[k]['sales'])
        collection_medians.append(median)
        collection_counts.append(count)

    # Weighted Median
    x = np.array(collection_medians)
    log.info(f'medians: {x}')
    weights = np.array(collection_counts)
    log.info(f'weights: {weights}')

    sudo_index = weighted_median(x, weights)
    log.info(sudo_index)

    usd_index = ((Decimal(str(sudo_index))/(10**18)) / Decimal(str(eth_rate))).quantize(Decimal('1.00'))

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    with conn.cursor() as cur:
        row_values = (event_date, sudo_index, "top50", usd_index, eth_rate)
        cur.execute('INSERT INTO nft.sudo_index (`event_date`,`price`,`index_type`,`usd_price`,`eth_rate`) VALUES (%s, %s, %s, %s, %s)', row_values)

        conn.commit()

        log.info("rds updated top50")
        conn.close()

    collection_sum = {}
    for i in result:
        if i[0] not in collection_sum:
            collection_sum[i[0]] = [i[1]]
        else:
            collection_sum[i[0]] = collection_sum[i[0]] + [i[1]]

    collection_medians = []
    collection_counts = []

    for i in collection_sum:
        median = statistics.median(collection_sum[i])
        count = len(collection_sum[i])
        collection_medians.append(median)
        collection_counts.append(count)

    # Weighted Median
    x = np.array(collection_medians)
    log.info(f'medians: {x}')
    weights = np.array(collection_counts)
    log.info(f'weights: {weights}')

    sudo_index = weighted_median(x, weights)
    log.info(sudo_index)

    usd_index = ((Decimal(str(sudo_index))/(10**18)) / Decimal(str(eth_rate))).quantize(Decimal('1.00'))

    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name, connect_timeout=15)
    with conn.cursor() as cur:
        row_values = (event_date, sudo_index, 'all', usd_index, eth_rate)
        cur.execute('INSERT INTO nft.sudo_index (`event_date`,`price`, `index_type`, `usd_price`, `eth_rate`) VALUES (%s, %s, %s, %s, %s)', row_values)

        conn.commit()

        log.info("rds updated all")


def weighted_median(data, weights):
    """
    Args:
      data (list or numpy.array): data
      weights (list or numpy.array): weights
    """
    data, weights = np.array(data).squeeze(), np.array(weights).squeeze()
    s_data, s_weights = map(np.array, zip(*sorted(zip(data, weights))))
    midpoint = 0.5 * sum(s_weights)
    if any(weights > midpoint):
        w_median = (data[weights == np.max(weights)])[0]
    else:
        cs_weights = np.cumsum(s_weights)
        idx = np.where(cs_weights <= midpoint)[0][-1]
        if cs_weights[idx] == midpoint:
            w_median = np.mean(s_data[idx:idx + 2])
        else:
            w_median = s_data[idx + 1]
    return w_median

