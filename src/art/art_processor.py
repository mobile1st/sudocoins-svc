import boto3
import json
from util import sudocoins_logger
from art.art import Art
import mimetypes
import http.client
from urllib.parse import urlparse

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
art = Art(dynamodb)


def lambda_handler(event, context):
    log.debug('art processor called')
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')
    stream_to_s3(data)
    log.info('record updated')


def stream_to_s3(data):
    art_table = dynamodb.Table('art')
    art_table.update_item(
        Key={'art_id': data['art_id']},
        UpdateExpression="SET process_status=:ps",
        ExpressionAttributeValues={
            ':ps': "attempted"
        },
        ReturnValues="UPDATED_NEW"
    )
    file = download(data['art_url'])

    s3_bucket = "art-processor-bucket"
    s3_file_path = data['art_id'] + extension(file['mimeType'])
    client = boto3.client('s3')
    client.put_object(Bucket=s3_bucket, Body=file['bytes'], Key=s3_file_path, ContentType=file['mimeType'])
    log.info('upload to s3 finished')

    art_table.update_item(
        Key={'art_id': data['art_id']},
        UpdateExpression="SET file_type=:ft, size=:size, process_status=:ps",
        ExpressionAttributeValues={
            ':ft': file['mimeType'],
            ':size': len(file['bytes']),
            ':ps': "processed"
        },
        ReturnValues="UPDATED_NEW"
    )
    log.info("art file type and size added to art table")

    return


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
