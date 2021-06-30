import boto3
import json
from util import sudocoins_logger
from art.art import Art
from botocore.vendored import requests

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

    response = requests.get(art_url, stream=True)
    log.info(response.headers)
    file_type = response.headers['content-type']
    type_index = file_type.find('/')
    file_ending = file_type[type_index + 1:]

    s3_bucket = "artprocessor"
    s3_file_path = data['art_id'] + '.' + file_ending
    s3 = boto3.client('s3')

    response.raw.decode_content = True
    conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
    s3.upload_fileobj(response.raw, s3_bucket, s3_file_path, Config=conf)

    log.info('streaming finished')

    return
