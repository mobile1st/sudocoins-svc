import sys
import pymysql
import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()

rds_host = "rds-proxy-read-only.endpoint.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
name = "admin"
password = "RHV2CiqtjiZpsM11"
db_name = "nft_events"
port = 3306


def lambda_handler(event, context):

    try:
        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)
    except Exception as e:
        sys.exit()

    with conn.cursor() as cur:
        sql = "select collection_id, sum(price) as a, count(*) as b, count(distinct buyer) as c from nft_events.open_sea_events where created_date >= now() - interval 1 day group by collection_id order by a desc limit 100;"

    cur.execute(sql)
    result = cur.fetchall()

    collection_list = []
    for i in result:
        tmp = {
            "collection_id": i[0],
            "sales_volume": i[1],
            "trades": i[2],
            "buyers": i[3]
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

    config_table = dynamodb.Table('Config')
    config_table.update_item(
        Key={
            'configKey': 'TopCollections'
        },
        UpdateExpression="set day=:day",
        ExpressionAttributeValues={
            ":day": collection_list
        },
        ReturnValues="ALL_NEW"
    )

    return



