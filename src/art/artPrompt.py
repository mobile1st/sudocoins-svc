import boto3
import sudocoins_logger
from art.art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    share_id = event['rawPath'].replace('/art/share/', '')
    art_uploads_record = art.get_by_share_id(share_id)

    return art_uploads_record
