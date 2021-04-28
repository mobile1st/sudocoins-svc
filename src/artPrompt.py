import boto3
import sudocoins_logger
from art import Art

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):

    art_uploads_record = art.get_by_share_id(event["shareId"])

    return art_uploads_record
