import boto3
import json
from util import sudocoins_logger
import mimetypes
import http.client
from urllib.parse import urlparse

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
cs_client = boto3.client('cloudsearchdomain',
                      endpoint_url='https://search-art-domain-oemytuqtulkq5plos7ri5qhz7a.us-west-2.cloudsearch.amazonaws.com')


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
    art_item = art_table.update_item(
        Key={'art_id': art_id},
        UpdateExpression="SET mime_type=:mt, cdn_url=:cdn_url REMOVE process_status",
        ExpressionAttributeValues={
            ':mt': file['mimeType'],
            ':cdn_url': f'https://cdn.sudocoins.com/{s3_file_path}'
        },
        ReturnValues='ALL_NEW'
    )['Attributes']
    log.info(f"art table updated artId: {art_id}")
    upload_art_to_cloudsearch(art_item)
    log.info(f"cloudsearch updated artId: {art_id}")


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


def upload_art_to_cloudsearch(art_item):
    art_id = art_item['art_id']
    name = art_item.get('name')
    data = art_item.get('open_sea_data', {})
    desc = data.get('description')
    upload_document(get_document(art_id, name, desc))


def upload_document(doc):
    response = cs_client.upload_documents(
        documents=json.dumps(doc),
        contentType='application/json'
    )
    log.info(f'cloudsearch upload response: {response}')


def get_document(art_id, name, description):
    log.info(f'cloudsearch document input art_id: {art_id}, name: {name}, desc: {description}')
    fields = {
        'category': 'art'
    }
    if name:
        fields['name'] = name
    if description:
        fields['description'] = description
    return [{
        'id': art_id,
        'type': 'add',
        'fields': fields
    }]
