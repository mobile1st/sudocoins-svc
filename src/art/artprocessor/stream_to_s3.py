import boto3
import json
from util import sudocoins_logger
import mimetypes
import http.client
from urllib.parse import urlparse
from util.sudocoins_encoder import SudocoinsEncoder

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')


def lambda_handler(event, context):
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')

    if 'STREAM_TO_S3' == data.get('process_status'):
        stream_to_s3(data['art_id'], data['art_url'])
    else:
        log.info(f'unsupported process type for: {data.get("process_status")}')


def stream_to_s3(art_id: str, art_url: str):
    file = download(art_url)

    s3_bucket = 'sudocoins-art-bucket'
    s3_file_path = art_id + extension(file['mimeType'])
    s3_client.put_object(Bucket=s3_bucket, Body=file['bytes'], Key=s3_file_path, ContentType=file['mimeType'])
    log.info('upload to sudocoins-art-bucket finished')

    art_table = dynamodb.Table('art')
    process_status = 'REKOGNITION_START'
    art_item = art_table.update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET mime_type=:mt, cdn_url=:cdn_url, process_status=:ps",
        ExpressionAttributeValues={
            ':mt': file['mimeType'],
            ':cdn_url': f'https://cdn.sudocoins.com/{s3_file_path}',
            ':ps': process_status
        },
        ReturnValues='ALL_NEW'
    )['Attributes']
    log.info(f"art table updated artId: {art_id}")
    sns_client.publish(
        TopicArn='arn:aws:sns:us-west-2:977566059069:ArtProcessor',
        MessageStructure='string',
        MessageAttributes={
            'art_id': {
                'DataType': 'String',
                'StringValue': art_id
            },
            'art_url': {
                'DataType': 'String',
                'StringValue': art_url
            },
            'process': {
                'DataType': 'String',
                'StringValue': process_status
            }
        },
        Message=json.dumps(art_item, cls=SudocoinsEncoder)
    )
    log.info(f'{art_id} published process status: {process_status}')


def extension(mime_type: str):
    key = mime_type.split(';')[0]
    ext = mimetypes.guess_extension(key)
    return ext if ext else ''


def get_request(url):
    url = urlparse(url)
    conn = http.client.HTTPSConnection(url.hostname, timeout=30)
    conn.request("GET", url.path)
    return conn.getresponse()


def get_with_redirects(url):
    response = get_request(url)
    while 300 <= response.status <= 400:
        location = response.getheader('Location')
        log.info(f'download {response.status} redirect to: {location}')
        response = get_request(location)
    if response.status > 400:
        raise Exception(f'download failed: {response.status} content: {response.read()}')

    return response


def download(url: str):
    log.info(f'download start {url}')
    response = get_with_redirects(url)
    length = response.getheader('Content-Length')
    content_type = response.getheader('Content-Type')
    log.info(f'download status: {response.status} starting to read: {content_type} {length if length else "?"} bytes')
    content_bytes = response.read()
    log.info(f'download success')
    return {'mimeType': content_type, 'bytes': content_bytes}

