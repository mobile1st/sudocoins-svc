import boto3
import json

from art.artprocessor import file_signatures
from util import sudocoins_logger
import mimetypes
import http.client
from urllib.parse import urlparse, quote, unquote_plus, parse_qsl, urlencode
from util.sudocoins_encoder import SudocoinsEncoder

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
art_table = dynamodb.Table('art')


def lambda_handler(event, context):
    data = json.loads(event['Records'][0]['Sns']['Message'])
    log.info(f'payload: {data}')

    if 'STREAM_TO_S3' == data.get('process_status'):
        safe_stream_to_s3(data['art_id'], data['art_url'])
    else:
        log.info(f'unsupported process type for: {data.get("process_status")}')


def safe_stream_to_s3(art_id, art_url):
    try:
        stream_to_s3(art_id, art_url)
    except Exception as e:
        item = art_table.get_item(Key={'art_id': art_id}).get('Item', {})
        image_url = item.get("open_sea_data", {}).get("image_url")
        log.info(f'RETRY download using {art_url} cause: {e}')
        try:
            stream_to_s3(art_id, image_url)
        except Exception as e:
            log.info(f'FAILED to download {art_id} url: {image_url} {e}')


def stream_to_s3(art_id: str, art_url: str):
    if not art_url:
        raise Exception(f'download failed: empty art url for art_id: {art_id}')

    file = download(art_url)

    s3_bucket = 'sudocoins-art-bucket'
    s3_extension, s3_mime_type = guess_extension(file['mimeType'], file['bytes'])
    s3_file_path = art_id + s3_extension
    log.info(f'upload to sudocoins-art-bucket {s3_mime_type} {s3_file_path}')
    s3_client.put_object(Bucket=s3_bucket, Body=file['bytes'], Key=s3_file_path, ContentType=s3_mime_type)
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


def guess_extension(mime_type: str, content):
    key = mime_type.split(';')[0]
    mime_ext = mimetypes.guess_extension(key)
    signature = file_signatures.type_from_sig(content)
    sig_ext = '.' + signature.get('file_extension') if signature else None
    sig_type, _ = mimetypes.guess_type('x' + sig_ext if sig_ext else '')

    if sig_ext and mime_ext:
        if sig_ext != mime_ext:
            return sig_ext, sig_type
        else:
            return mime_ext, mime_type
    elif sig_ext and sig_type:
        return sig_ext, sig_type
    elif mime_ext:
        return mime_ext, mime_ext
    else:
        return ''


def get_request(url):
    url = urlparse(url)
    conn = http.client.HTTPSConnection(url.hostname, timeout=10)
    query = extract_query(url)
    log.debug(f'GET {query}')
    conn.request("GET", query)
    return conn.getresponse()


def get_with_redirects(url):
    response = get_request(url)
    prev_location = url
    while 300 <= response.status <= 400:
        location = response.getheader('Location')
        if not location:
            raise Exception(f'download failed: redirect has no target location content: {response.read()}')
        if location == prev_location:
            raise Exception(f'download failed: infinite redirect loop for url: {location} content: {response.read()}')

        if location.startswith('/'):
            p = urlparse(prev_location)
            location = url[0:prev_location.find('/', prev_location.find(p.hostname))] + location

        log.info(f'download {response.status} redirect to: {location}')
        response = get_request(location)
        prev_location = location
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


def extract_query(parsed_url):
    query = parsed_url.query
    prev_query = None
    while prev_query != query:
        prev_query = query
        query = unquote_plus(query)

    query = urlencode(parse_qsl(query))
    return quote(parsed_url.path) + ('?' + query if query else '')
