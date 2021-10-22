import boto3
from util import sudocoins_logger
import string

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
        key_list.append(i.lower())

    query = {
        'Keys': key_list,
        'ProjectionExpression': 'collections'
    }
    response = dynamodb.batch_get_item(RequestItems={'search': query})

    collections = []
    for k in response['Responses']['search']:
        collections.extend(k)

    unique_collection = set(collections)
    collections = list(unique_collection)


    return {
        'arts': collections
    }


def extract_parameters(query_params):
    query = query_params['q']
    words = query.split(" ")

    return words


def set_log_context(event):
    global log
    log = sudocoins_logger.get(sudocoins_logger.get_ctx(event))


