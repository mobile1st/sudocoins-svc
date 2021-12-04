import boto3
from util import sudocoins_logger
from datetime import datetime
import json

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    set_log_context(event)
    log.info(f'event: {event}')
    body = json.loads(event.get('body', '{}'))

    file_name = body['file_name']
    art_id = file_name.split('.')[0]

    try:

        form_data = {
            "upcoming_id": art_id,
            "timestamp": str(datetime.utcnow().isoformat()),
            "name": body.get("name"),
            "description": body.get("description"),
            "preview_url": f'https://cdn.sudocoins.com/{file_name}',
            "twitter": body.get("twitter"),
            "discord": body.get("discord"),
            "instagram": body.get("instagram"),
            "website": body.get("website"),
            "opensea": body.get("opensea"),
            "total": body.get("total"),
            "mint_price": body.get("mint_price"),
            "mint_currency": body.get("mint_currency"),
            "release_date": body.get("release_date"),
            "release_time": body.get("release_time"),
            "presale_time": body.get("presale_time"),
            "presale_date": body.get("presale_date"),
            "blockchain": body.get("blockchain"),
            "approved": "false",
            "presale_total": body.get("presale_total")
        }

        dynamodb.Table('upcoming').put_item(
            Item=form_data
        )

        return {
            "status": 200
        }

    except Exception as e:
        log.info(e)

        return {
            "status": 500
        }



def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))