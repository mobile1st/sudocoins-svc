import boto3
import sudocoins_logger
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    try:
        user_id = event['userId']
        art_id = event['art_id']

        shareId = art.share(art_id, user_id )

        return shareId

    except Exception as e:
        log.exception(e)



