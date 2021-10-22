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

    return {
        'arts': get_collection_data(collections)
    }


def extract_parameters(query_params):
    query = query_params['q']
    words = query.split(" ")

    return words


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


def get_collection_data(collections):
    key_list = []
    for i in collections:
        key_list.append({"collection_id": i})

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collection_id, sale_count, sales_volume, collection_name, preview_url'
    }
    log.info(query)

    response = dynamodb.batch_get_item(RequestItems={'search': query})

    collection_objects = response['Responses']['search']

    return collection_objects


