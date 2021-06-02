import boto3
import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug(f'art_prompt {event}')
    share_id = event['rawPath'].replace('/art/share/', '')
    source_ip = event['requestContext']['http']['sourceIp']
    art_uploads_record = art.get_by_share_id(source_ip, share_id)

    return art_uploads_record
