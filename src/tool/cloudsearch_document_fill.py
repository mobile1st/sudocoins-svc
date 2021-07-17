import json
import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
cs_client = boto3.client('cloudsearchdomain',
                         endpoint_url='https://search-art-domain-oemytuqtulkq5plos7ri5qhz7a.us-west-2.cloudsearch.amazonaws.com')
rekognition_client = boto3.client('rekognition')
cdn_url_prefix = 'https://cdn.sudocoins.com/'


def fill_search_domain(update_image_only=False):
    arts = get_arts()

    for item in arts:
        try:
            art_id = item['art_id']
            name = item.get('name')
            data = item.get('open_sea_data', {})
            desc = data.get('description')
            mime_type = item.get('mime_type')
            cdn_url = item.get('cdn_url')
            if update_image_only:
                if mime_type and cdn_url and (mime_type == 'image/jpeg' or mime_type == 'image/png'):
                    tags = get_rekognition_labels(cdn_url.replace(cdn_url_prefix, ''))
                    upload_document(get_document(art_id, name, desc, tags))
                else:
                    continue
            else:
                upload_document(get_document(art_id, name, desc))
        except Exception as e:
            log.exception(f'exception during CloudSearch upload: {item}, cause: {e}')


def get_arts():
    art_table = dynamodb.Table('art')
    arts = []
    scan_kwargs = {}
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = art_table.scan(**scan_kwargs)
        arts.extend(response.get('Items', []))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
    return arts


def search(query):
    response = cs_client.search(
        # cursor='string',
        # expr='string',
        # facet='string',
        # filterQuery='string',
        # highlight='string',
        # partial=True | False,
        query=query,
        # queryOptions='string',
        # queryParser='simple' | 'structured' | 'lucene' | 'dismax',
        # returnFields='string',
        # size=123,
        # sort='string',
        # start=123,
        # stats='string'
    )
    log.info(response)
    return response


def upload_document(doc):
    response = cs_client.upload_documents(
        documents=json.dumps(doc),
        contentType='application/json'
    )
    # log.info(response)
    return response


def get_document(art_id, name, description, tags=None):
    log.info(f'art_id: {art_id}, name: {name}, desc: {description}, tags: {tags}')
    fields = {
        'category': 'art'
    }
    if name:
        fields['name'] = name
    if description:
        fields['description'] = description
    if tags:
        fields['tags'] = tags
    return [{
        'id': art_id,
        'type': 'add',
        'fields': fields
    }]


def get_rekognition_labels(art_s3_name):
    response = rekognition_client.detect_labels(
        Image={
            'S3Object': {
                'Bucket': 'sudocoins-art-bucket',
                'Name': art_s3_name
            }
        },
        MinConfidence=70
    )
    # print(json.dumps(response, indent=4))
    return ','.join([label['Name'] for label in response['Labels']])


def manual_fill():
    upload_document(get_document('4f86dc44-e504-11eb-b726-5f2808d15351', None, None, 'basketball,man,human'))


def print_rek_labels():
    print(get_rekognition_labels('17d01f1b-cc24-11eb-97cf-55181c532c03.png'))


fill_search_domain(True)
