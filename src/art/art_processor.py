import boto3
import json
from util import sudocoins_logger
import mimetypes
import http.client
from urllib.parse import urlparse

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
cdn_prefix = 'https://cdn.sudocoins.com/'


def lambda_handler(event, context):
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')

    if 'STREAM_TO_S3' == data.get('process'):
        stream_to_s3(data)
    else:
        log.info(f'unsupported process type for: {data.get("process")}')


def stream_to_s3(data):
    file = download(data['art_url'])

    s3_bucket = 'sudocoins-art-bucket'
    s3_file_path = data['art_id'] + extension(file['mimeType'])
    s3_client.put_object(Bucket=s3_bucket, Body=file['bytes'], Key=s3_file_path, ContentType=file['mimeType'])
    log.info('upload to s3 finished')

    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': data['art_id']},
        UpdateExpression="SET file_type=:ft, size=:size, cdn_url=:cdn_url",
        ExpressionAttributeValues={
            ':ft': file['mimeType'],
            ':size': len(file['bytes']),  # TODO remove, irrelevant after URL rewrite
            ':cdn_url': f'{cdn_prefix}{s3_file_path}'
        })
    art_table.update_item(
        Key={'art_id': data['art_id']},
        UpdateExpression="remove process_status")

    log.info("art table updated")


def extension(mime_type: str):
    key = mime_type.split(';')[0]
    ext = mimetypes.guess_extension(key)
    return ext if ext else ''


def download(url: str):
    log.info(f'download {url}')
    url = urlparse(url)
    conn = http.client.HTTPSConnection(url.hostname, timeout=10)
    conn.request("GET", url.path)
    response = conn.getresponse()
    length = response.getheader('Content-Length')
    content_type = response.getheader('Content-Type')
    log.info(f'download status: {response.status} starting to read: {content_type} {length if length else "?"} bytes')
    content_bytes = response.read()
    log.info(f'download success')
    return {'mimeType': content_type, 'bytes': content_bytes}
