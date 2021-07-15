import json
import boto3
from util import sudocoins_logger

log = sudocoins_logger.get()
dynamodb = boto3.resource('dynamodb')
client = boto3.client('cloudsearchdomain',
                      endpoint_url='https://search-art-domain-oemytuqtulkq5plos7ri5qhz7a.us-west-2.cloudsearch.amazonaws.com')


def fill_search_domain():
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
    for item in arts:
        try:
            print(item)
            art_id = item['art_id']
            name = item.get('name')
            data = item.get('open_sea_data', {})
            desc = data.get('description')
            upload_document(get_document(art_id, name, desc))
        except Exception as e:
            log.exception(f'exception during CloudSearch upload: {item}, cause: {e}')


def search(query):
    response = client.search(
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
    response = client.upload_documents(
        documents=json.dumps(doc),
        contentType='application/json'
    )
    log.info(response)
    return response


def get_document(art_id, name, description):
    log.info(f'art_id: {art_id}, name: {name}, desc: {description}')
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


fill_search_domain()
