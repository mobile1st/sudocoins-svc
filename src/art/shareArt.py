import boto3
import json
import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug(event)
    try:
        body = json.loads(event['body'])
        user_id = body['userId']
        art_id = body['art_id']
        log.info(f'artId: {art_id}')
        share_id = art.share(user_id, art_id)

        return share_id

    except Exception as e:
        log.exception(e)



