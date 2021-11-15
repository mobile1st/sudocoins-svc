import json
import sys
import logging
import pymysql


def lambda_handler(event, context):
    rds_host = "rds-proxy-read-only.endpoint.proxy-ccnnpquqy2qq.us-west-2.rds.amazonaws.com"
    name = "admin"
    password = "RHV2CiqtjiZpsM11"
    db_name = "nft_events"
    port = 3306

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)

    except Exception as e:
        logger.error(e)
        sys.exit()

    print("SUCCESS: Connection to RDS mysql instance succeeded")

    return event