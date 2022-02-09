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
    collection_code = art['collection_code']
    twitter = art['twitter']

    last_update = str(datetime.utcnow().isoformat())

    try:
        collection_record = dynamodb.Table('collections').get_item(
            Key={'collection_id': collection_code}
        )

    except Exception as e:
        log.info(e)

    return