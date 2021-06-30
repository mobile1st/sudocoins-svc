import boto3
import json
from util import sudocoins_logger
from art.art import Art
import requests


log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('art processor called')
    for record in event['Records']:
        payload = record['body']
        log.info(f'payload: {payload}')

        data = json.loads(payload)
        stream_to_s3(data)

        log.info('record updated')


def stream_to_s3(data):
    art_url = data['art_url']
    session = requests.Session()
    response = session.get(art_url, stream=True)

    s3_bucket = "artprocessor"
    s3_file_path = data['art_id']
    s3 = boto3.client('s3')
    with response as part:
        part.raw.decode_content = True
        conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
        s3.upload_fileobj(part.raw, s3_bucket, s3_file_path, Config=conf)

    log.info('streaming finished')

    return
