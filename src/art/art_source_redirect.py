import boto3
from botocore.config import Config
import json
from util import sudocoins_logger
import uuid
from datetime import datetime

log = sudocoins_logger.get()
config = Config(connect_timeout=0.1, read_timeout=0.1, retries={'max_attempts': 5, 'mode': 'standard'})
dynamodb = boto3.resource('dynamodb', config=config)


def lambda_handler(event, context):
    set_log_context(event)
    log.debug(f'event: {event}')
    if event.get('queryStringParameters') is None:
        log.warn('the request does not contain query parameters')
        return {
            "statusCode": 302,
            "headers": {'Location': 'https://www.sudocoins.com/?msg=invalid'},
            "body": json.dumps({})
        }
    params = event['queryStringParameters']
    row_id = params["id"]

    art_uploads_table = dynamodb.Table('art_uploads')
    art_table = dynamodb.Table('art')

    art_row = art_table.get_item(Key={'art_id': row_id})

    if 'Item' in art_row:

        art_table.update_item(
            Key={'art_id': row_id},
            UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
            ExpressionAttributeValues={
                ':inc': 1,
                ':start': 0
            },
            ReturnValues="UPDATED_NEW"
        )
        log.info("art table click_count increased")
        art_votes_record = {
            "unique_id": str(uuid.uuid1()),
            "art_id": row_id,
            "timestamp": str(datetime.utcnow().isoformat()),
            "type": "buy"
        }
        dynamodb.Table('art_votes').put_item(
            Item=art_votes_record
        )
        log.info("record added to art_votes table")

        buy_url = art_row['Item']['buy_url']

    else:
        row = art_uploads_table.get_item(Key={'shareId': row_id})

        if 'Item' in row:
            art_uploads_table.update_item(
                Key={'shareId': row_id},
                UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues="UPDATED_NEW"
            )
            log.info("art_uploads table click_count increased")

            art_id = row['Item']['art_id']

            art_table.update_item(
                Key={'art_id': art_id},
                UpdateExpression="SET click_count = if_not_exists(click_count , :start) + :inc",
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':start': 0
                },
                ReturnValues="UPDATED_NEW"
            )
            log.info("art table click_count increased")
            art_votes_record = {
                "unique_id": str(uuid.uuid1()),
                "art_id": art_id,
                "timestamp": str(datetime.utcnow().isoformat()),
                "type": "buy"
            }
            dynamodb.Table('art_votes').put_item(
                Item=art_votes_record
            )
            log.info("record added to art_votes table")

            buy_url = row['Item']['buy_url']

    response = {
        "statusCode": 302,
        "headers": {'Location': (buy_url+'?ref=0x883a77493c18217d38149139951815a4d8d721ca')},
        "body": json.dumps({})
    }

    return response


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))
