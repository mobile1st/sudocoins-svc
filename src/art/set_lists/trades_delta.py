import sys
import pymysql
import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')

rds_host = "rds-proxy.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
name = "admin"
password = "RHV2CiqtjiZpsM11"
db_name = "nft_events"
port = 3306
conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)


def lambda_handler(event, context):

    day = get_collections()

    config_table = dynamodb.Table('Config')
    config_table.update_item(
        Key={
            'configKey': 'TradesDelta'
        },
        UpdateExpression="set #d=:d",
        ExpressionAttributeValues={
            ":d": day
        },
        ReturnValues="ALL_NEW",
        ExpressionAttributeNames={'#d': 'day'}
    )

    return


def get_collections():
    with conn.cursor() as cur:
        sql = "SELECT distinct t1.collection_id, t2.day2, t1.day,  (t1.day-t2.day2)/t2.day2*100 AS a FROM (SELECT collection_id, COUNT(*) AS day FROM nft_events.open_sea_events where event_date >= now() - interval 1 day GROUP BY collection_id) t1 LEFT JOIN (SELECT collection_id, COUNT(*) AS day2 FROM nft_events.open_sea_events where event_date >= now() - interval 2 day and event_date <= now() - interval 1 day GROUP BY collection_id) t2 ON t1.collection_id = t2.collection_id where t2.day2 > 10 order by a desc limit 100;"
        cur.execute(sql)
        result = cur.fetchall()

    collection_list = []
    for i in result:
        tmp = {
            "collection_id": i[0],
            "yesterday": i[1],
            "today": i[2],
            "delta": i[3]
        }
        collection_list.append(tmp)

    dynamodb = boto3.resource('dynamodb')

    key_list = []
    for i in collection_list:
        tmp = {
            "collection_id": i['collection_id']
        }

        key_list.append(tmp)

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, preview_url, collection_name, chart_data'
    }
    response = dynamodb.batch_get_item(RequestItems={'collections': query})

    for i in response['Responses']['collections']:
        for k in range(len(collection_list)):
            if i['collection_id'] == collection_list[k]['collection_id']:
                collection_list[k].update(i)
                continue

    return collection_list
