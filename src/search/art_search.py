import boto3
from util import sudocoins_logger
import string
from boto3.dynamodb.conditions import Key

log = sudocoins_logger.get()
google_search_host = 'customsearch.googleapis.com'
search_engine_id = 'abe73c1ca8f9839de'
api_key = 'AIzaSyA4Be7eS9trAjcz5S4nkxPxKhhpC2IEP6E'
art_page_prefix = 'https://www.sudocoins.com/art/'
dynamodb = boto3.resource('dynamodb')


def lambda_handler(event, context):
    log.debug(f'event: {event}')
    query = extract_parameters(event['queryStringParameters'])
    log.info(query)

    key_list = []
    for i in query:
        key_list.append({"search_key": i.lower()})

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collections'
    }
    log.info(query)

    response = dynamodb.batch_get_item(RequestItems={'search': query})
    log.info(response)

    collections = set([])
    for k in response['Responses']['search']:
        tmp_set = k['collections']
        collections.update(tmp_set)

    log.info(collections)

    '''
    uploads = []
    for i in collections:
        data = dynamodb.Table('art').query(
        KeyConditionExpression=Key('collection_id').eq(i),
        ScanIndexForward=False,
        IndexName='collection_id-last_sale_price-index',
        ProjectionExpression='art_id',
        Limit=20
    )

        uploads.append(data['Items'])

    arts = []

    for i in uploads:
        for k in i:
            arts.append(k['art_id'])
    '''
    return {
        'arts': list(collections)
    }


def extract_parameters(query_params):
    query = query_params['q']
    words = query.split(" ")

    return words


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


